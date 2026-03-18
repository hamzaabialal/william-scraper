import asyncio
import re
import time
import os
import json

import pandas as pd
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials


# ─── Google Sheets helpers ────────────────────────────────────────────────────

def get_gsheet_client():
    """Authenticate with Google Sheets using service account JSON from env var."""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json.strip():
        raise ValueError(
            "GOOGLE_CREDENTIALS_JSON secret is empty or not set. "
            "Go to your GitHub repo → Settings → Secrets and variables → Actions "
            "and add GOOGLE_CREDENTIALS_JSON with the full contents of your service account JSON key."
        )
    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


COLUMNS = [
    "Date d'envoi", "No. Centris", "Lien", "Address", "Prix",
    "TGA Demandé", "Rev résidentiel", "Rev commercial", "Rev parking",
    "Rev autres", "Taxes municipales", "Taxe scolaire", "Électricité",
    "Mazout", "Gaz", "Assurances", "Typologie",
]


def ensure_header(worksheet):
    """Write the header row if the sheet is empty or has no header yet."""
    first_row = worksheet.row_values(1)
    if first_row != COLUMNS:
        if not first_row:
            # Sheet is completely empty — insert header at row 1
            worksheet.insert_row(COLUMNS, index=1)
        else:
            print("WARNING: Row 1 does not match expected headers. Headers found:", first_row)


def load_existing_centris_ids(worksheet) -> set:
    """Return the set of Centris IDs already in the sheet (column 'No. Centris')."""
    all_values = worksheet.get_all_values()
    if len(all_values) < 2:
        return set()  # empty or only header
    header = all_values[0]
    try:
        col_idx = header.index("No. Centris")
    except ValueError:
        print("WARNING: 'No. Centris' column not found in sheet header.")
        return set()
    existing = set()
    for row in all_values[1:]:
        if col_idx < len(row):
            val = str(row[col_idx]).strip()
            if val:
                existing.add(val)
    print(f"Existing Centris IDs in sheet: {existing}")
    return existing


def append_rows_to_sheet(worksheet, df: pd.DataFrame):
    """Append new rows to the Google Sheet."""
    if df.empty:
        return
    rows = df.values.tolist()
    # Convert any non-string types to string to avoid gspread serialisation issues
    cleaned = [[str(cell) if not isinstance(cell, (int, float)) else cell for cell in row] for row in rows]
    worksheet.append_rows(cleaned, value_input_option="USER_ENTERED")


# ─── Scraper helpers ──────────────────────────────────────────────────────────

def get_data_dict(lines):
    # Revenue
    data_dict = {}
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

async def scrape(matrix_pages: list[str], existing_ids: set) -> pd.DataFrame:
    columns = [
        "Date d'envoi", "No. Centris", "Lien", "Address", "Prix",
        "TGA Demandé", "Rev résidentiel", "Rev commercial", "Rev parking",
        "Rev autres", "Taxes municipales", "Taxe scolaire", "Électricité",
        "Mazout", "Gaz", "Assurances", "Typologie",
    ]
    all_rows = []

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

            # Debug: screenshot so we can see what the page looks like in CI
            await page.screenshot(path="debug_after_goto.png", full_page=True)
            print("Page title after goto:", await page.title())
            print("Page URL after goto:", page.url)

            # Use contains match to avoid apostrophe encoding issues (U+2019 vs U+0027)
            try:
                await page.wait_for_selector('a[title*="affichages"]', timeout=30000)
            except Exception:
                await page.screenshot(path="debug_timeout.png", full_page=True)
                # Print all <a> title attributes to diagnose the exact text
                titles = await page.eval_on_selector_all('a[title]', 'els => els.map(e => e.title)')
                print("All <a title> values on page:", titles)
                print("Page HTML snippet:\n", (await page.content())[:3000])
                raise

            await page.click('a[title*="affichages"]')

            await page.wait_for_selector('a:has-text("Sommaire")', state='visible', timeout=7000)
            async with page.expect_navigation(wait_until='load', timeout=15000):
                await page.click('a:has-text("Sommaire")')

            centris_values = []

            while True:
                await page.wait_for_selector(':has-text("Revenu bruts potentiels")', timeout=10000)

                address = await page.locator('span.d-mega').inner_text()
                address = address.strip()

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

                centris_id_clean = int("".join(ch for ch in nb_centris if ch.isdigit()))

                if nb_centris in existing_ids or centris_id_clean in centris_values:
                    print(nb_centris, "Already in DB")
                    await page.click('a.glyphicon.glyphicon-chevron-right')
                    print("About to break while loop")
                    break
                else:
                    all_rows.append(new_row)
                    centris_values.append(centris_id_clean)
                    existing_ids.add(nb_centris)
                    print("Length of centris_values:", len(centris_values))
                    print(new_row)
                    await page.click('a.glyphicon.glyphicon-chevron-right')
                    time.sleep(5)

            await browser.close()

    df = pd.DataFrame(all_rows, columns=columns)
    # Serialize Typologie dict to string so it fits in a sheet cell
    if "Typologie" in df.columns:
        df["Typologie"] = df["Typologie"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x)
    return df


# ─── Entry point ─────────────────────────────────────────────────────────────

async def main():
    matrix_pages = [
        "https://matrix.centris.ca/Matrix/Public/Portal.aspx?L=2&k=7674282XFBM&p=AE-1549033-780#1",
    ]

    spreadsheet_id = os.environ["GOOGLE_SHEET_ID"]
    worksheet_name = os.environ.get("GOOGLE_SHEET_TAB", "Sheet1")

    client = get_gsheet_client()
    spreadsheet = client.open_by_key(spreadsheet_id)

    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)

    ensure_header(worksheet)
    existing_ids = load_existing_centris_ids(worksheet)
    print(f"Existing IDs in sheet: {len(existing_ids)}")

    df = await scrape(matrix_pages, existing_ids)
    print(f"New rows scraped: {len(df)}")

    if not df.empty:
        append_rows_to_sheet(worksheet, df)
        print("Rows appended to Google Sheet.")
    else:
        print("No new rows to add.")


if __name__ == "__main__":
    asyncio.run(main())
