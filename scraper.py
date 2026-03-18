import asyncio
import re
import time
import os
import json

import pandas as pd
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe


# ─── Google Sheets auth ───────────────────────────────────────────────────────

def get_worksheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json.strip():
        raise ValueError(
            "GOOGLE_CREDENTIALS_JSON secret is empty or not set. "
            "Add it under GitHub repo → Settings → Secrets and variables → Actions."
        )
    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)

    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    worksheet = gc.open_by_key(sheet_id).worksheet("Sheet4")
    return worksheet


# ─── Scraper helpers ──────────────────────────────────────────────────────────

def get_data_dict(lines):
    data_dict = {}

    # Revenue
    data_dict["Rev résidentiel"] = ''.join(filter(str.isdigit, lines[0]))

    if "Commercial" in lines and "$" in lines[lines.index("Commercial") + 1]:
        data_dict["Rev commercial"] = ''.join(filter(str.isdigit, lines[lines.index("Commercial") + 1]))

    if "Stationnements/Garages" in lines and "$" in lines[lines.index("Stationnements/Garages") + 1]:
        data_dict["Rev parking"] = ''.join(filter(str.isdigit, lines[lines.index("Stationnements/Garages") + 1]))

    if "Autres" in lines and "$" in lines[lines.index("Autres") + 1]:
        data_dict["Rev autres"] = int(''.join(filter(str.isdigit, lines[lines.index("Autres") + 1])))

    # Depenses
    for i, line in enumerate(lines):
        if "municipale" in line.lower():
            try:
                if "$" in lines[i + 1]:
                    data_dict["Taxes municipales"] = ''.join(filter(str.isdigit, lines[i + 1]))
            except IndexError:
                pass
        if "scolaire" in line.lower():
            try:
                if "$" in lines[i + 1]:
                    data_dict["Taxe scolaire"] = ''.join(filter(str.isdigit, lines[i + 1]))
            except IndexError:
                pass

    if "Énergie - Électricité" in lines and "$" in lines[lines.index("Énergie - Électricité") + 1]:
        data_dict["Électricité"] = lines[lines.index("Énergie - Électricité") + 1]
    if "Énergie - Mazout" in lines and "$" in lines[lines.index("Énergie - Mazout") + 1]:
        data_dict["Mazout"] = lines[lines.index("Énergie - Mazout") + 1]
    if "Énergie - Gaz" in lines and "$" in lines[lines.index("Énergie - Gaz") + 1]:
        data_dict["Gaz"] = lines[lines.index("Énergie - Gaz") + 1]
    if "Assurances" in lines and "$" in lines[lines.index("Assurances") + 1]:
        data_dict["Assurances"] = lines[lines.index("Assurances") + 1]

    # Typologie
    typologie_dict = {}
    lines_typologie = lines[lines.index("Nombre d'unités"):]

    for key in ["Loft/Studio", "Chambres", "1 ½", "2 ½", "3 ½", "4 ½", "5 ½",
                "6 ½", "7 ½", "8 ½", "9 ½", "Autre", "Stationnements/Garages", "Commercial"]:
        label = {
            "1 ½": "1.5", "2 ½": "2.5", "3 ½": "3.5", "4 ½": "4.5",
            "5 ½": "5.5", "6 ½": "6.5", "7 ½": "7.5", "8 ½": "8.5",
            "9 ½": "9.5",
        }.get(key, key)
        if key in lines_typologie:
            try:
                typologie_dict[label] = lines_typologie[lines_typologie.index(key) + 1]
            except IndexError:
                pass

    def clean_value(val):
        if val is None:
            return 0
        cleaned = re.sub(r"[^0-9.]", "", str(val))
        return float(cleaned) if cleaned else 0

    columns = [
        "Date d'envoi", "No. Centris", "Lien", "Address", "Prix",
        "TGA Demandé", "Rev résidentiel", "Rev commercial", "Rev parking",
        "Rev autres", "Taxes municipales", "Taxe scolaire", "Électricité",
        "Mazout", "Gaz", "Assurances", "Typologie",
    ]
    new_row = {col: data_dict.get(col, 0) for col in columns}
    typologie_dict = {k: int(clean_value(v)) for k, v in typologie_dict.items()}
    new_row["Typologie"] = typologie_dict

    return new_row


# ─── Main scraper ─────────────────────────────────────────────────────────────

async def main():
    matrix_pages = [
        "https://matrix.centris.ca/Matrix/Public/Portal.aspx?ID=0-1679004740-00&eml=dy5ncmlnYXRAbGlmYS5jYQ==&L=2",
        "https://matrix.centris.ca/Matrix/Public/Portal.aspx?L=2&k=7674282XFBM&p=AE-1549033-780#1",
    ]

    worksheet = get_worksheet()

    # Load existing data from sheet — same as original Colab code
    saved_data = get_as_dataframe(worksheet, evaluate_formulas=True)
    saved_data = saved_data.dropna(how="all")  # drop fully empty rows gspread_dataframe adds
    print(f"Rows already in sheet: {len(saved_data)}")

    # Build centris_values list from sheet — same logic as original
    if "No. Centris" in saved_data.columns and len(saved_data) > 0:
        centris_values = [int(''.join(filter(str.isdigit, str(x)))) for x in saved_data["No. Centris"].values if str(x).strip() not in ("", "nan")]
    else:
        centris_values = []
    print("Length of centris_values:", len(centris_values))
    print(centris_values)

    for matrix_page in matrix_pages:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()
            await page.goto(matrix_page, wait_until="networkidle", timeout=60000)

            await page.screenshot(path="debug_after_goto.png", full_page=True)
            print("Page title after goto:", await page.title())
            print("Page URL after goto:", page.url)

            try:
                await page.wait_for_selector('a[title*="affichages"]', timeout=30000)
            except Exception:
                await page.screenshot(path="debug_timeout.png", full_page=True)
                titles = await page.eval_on_selector_all('a[title]', 'els => els.map(e => e.title)')
                print("All <a title> values on page:", titles)
                print("Page HTML snippet:\n", (await page.content())[:3000])
                raise

            await page.click('a[title*="affichages"]')

            await page.wait_for_selector('a:has-text("Sommaire")', state='visible', timeout=7000)
            async with page.expect_navigation(wait_until='load', timeout=15000):
                await page.click('a:has-text("Sommaire")')

            new_rows_count = 0

            while True:
                await page.wait_for_selector(':has-text("Revenu bruts potentiels")', timeout=10000)

                address = await page.locator('span.d-mega').inner_text()
                address = address.strip()
                address += ", Montreal"

                price = await page.locator('span.d-text.d-fontSize--larger').inner_text()
                price = ''.join(filter(str.isdigit, price))

                small_font = await page.locator('span.d-subtextSoft.d-fontSize--smallest').all_text_contents()
                nb_centris = small_font[0][13:21]
                date_envoi = small_font[0][-10:]

                start = page.locator('xpath=//*[contains(normalize-space(.), "Revenu bruts potentiels")]').nth(14)
                section_text = (await start.inner_text()).strip()
                lines = [l.strip() for l in section_text.splitlines() if l.strip()]
                lines = lines[2:]

                new_row = get_data_dict(lines)
                new_row["Address"] = address
                new_row["Prix"] = price
                new_row["No. Centris"] = nb_centris
                new_row["Date d'envoi"] = date_envoi
                new_row["Lien"] = "https://www.centris.ca/fr/propriete/" + str(nb_centris)

                if int("".join(ch for ch in nb_centris if ch.isdigit())) in centris_values:
                    print(nb_centris, "Already in DB")
                    await page.click('a.glyphicon.glyphicon-chevron-right')
                    print("About to break while loop")
                    break
                else:
                    # Serialize Typologie dict to string for the sheet cell
                    new_row["Typologie"] = json.dumps(new_row["Typologie"], ensure_ascii=False) if isinstance(new_row["Typologie"], dict) else new_row["Typologie"]

                    # Save this row immediately to Sheet4
                    row_values = [
                        new_row.get("Date d'envoi", ""),
                        new_row.get("No. Centris", ""),
                        new_row.get("Lien", ""),
                        new_row.get("Address", ""),
                        new_row.get("Prix", ""),
                        new_row.get("TGA Demandé", ""),
                        new_row.get("Rev résidentiel", ""),
                        new_row.get("Rev commercial", ""),
                        new_row.get("Rev parking", ""),
                        new_row.get("Rev autres", ""),
                        new_row.get("Taxes municipales", ""),
                        new_row.get("Taxe scolaire", ""),
                        new_row.get("Électricité", ""),
                        new_row.get("Mazout", ""),
                        new_row.get("Gaz", ""),
                        new_row.get("Assurances", ""),
                        new_row.get("Typologie", ""),
                    ]
                    worksheet.append_row(row_values, value_input_option="USER_ENTERED")
                    new_rows_count += 1

                    centris_values.append(nb_centris)
                    print(f"Saved to sheet ({new_rows_count} new so far):", nb_centris, address)
                    await page.click('a.glyphicon.glyphicon-chevron-right')
                    time.sleep(5)

            await browser.close()
        print(f"Done. Total new rows saved to Sheet4: {new_rows_count}")


if __name__ == "__main__":
    asyncio.run(main())
