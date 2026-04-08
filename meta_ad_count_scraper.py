import argparse
import os
import re
from urllib.parse import quote_plus

import gspread
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

load_dotenv()

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "shopify_brands_meta")
GOOGLE_CREDS_FILE = os.getenv(
    "GOOGLE_CREDS_FILE", os.path.join(os.path.dirname(__file__), "creds.json")
)

# Sheet columns (1-based)
BRAND_COLUMN_INDEX = 1  # Column A
AD_COUNT_COLUMN_INDEX = 9  # Column I

COUNTRY = "US"
MAX_SCROLLS = 14
SCROLL_PAUSE_MS = 1200
INITIAL_WAIT_MS = 4000

LIBRARY_ID_PATTERN = re.compile(r"Library ID\s*:?\s*(\d+)", re.IGNORECASE)


def get_sheet_client():
    if not GOOGLE_SHEET_ID:
        raise ValueError("Missing GOOGLE_SHEET_ID environment variable")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_WORKSHEET_NAME)


def sheet_tasks(sheet, force_refresh=False):
    rows = sheet.get_all_values()
    tasks = []

    for row_number, row in enumerate(rows, start=1):
        if row_number == 1:
            continue

        brand_name = row[BRAND_COLUMN_INDEX - 1].strip() if len(row) >= BRAND_COLUMN_INDEX else ""
        existing_count = row[AD_COUNT_COLUMN_INDEX - 1].strip() if len(row) >= AD_COUNT_COLUMN_INDEX else ""

        if not brand_name:
            continue

        if not force_refresh and existing_count:
            continue

        tasks.append((row_number, brand_name))

    return tasks


def build_ads_library_url(brand_name):
    encoded = quote_plus(brand_name)
    return (
        "https://www.facebook.com/ads/library/?"
        f"active_status=all&ad_type=all&country={COUNTRY}&search_type=keyword_unordered&q={encoded}"
    )


def collect_library_ids_from_page(page):
    ads = page.locator("div:has-text('See ad details')")
    ids = set()

    try:
        texts = ads.all_inner_texts()
    except Exception:
        texts = []

    for text in texts:
        for match in LIBRARY_ID_PATTERN.findall(text):
            ids.add(match)

    return ids, ads.count()


def get_meta_ad_count_for_brand(page, brand_name):
    url = build_ads_library_url(brand_name)
    page.goto(url, wait_until="domcontentloaded")
    page.wait_for_timeout(INITIAL_WAIT_MS)

    unique_ids = set()
    prev_visible_count = -1
    stable_rounds = 0

    for _ in range(MAX_SCROLLS):
        current_ids, visible_count = collect_library_ids_from_page(page)
        unique_ids.update(current_ids)

        if visible_count == prev_visible_count:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= 2:
            break

        prev_visible_count = visible_count
        page.mouse.wheel(0, 9000)
        page.wait_for_timeout(SCROLL_PAUSE_MS)

    if unique_ids:
        return len(unique_ids)

    fallback_count = page.locator("div:has-text('See ad details')").count()
    return fallback_count


def main():
    parser = argparse.ArgumentParser(description="Scrape Meta ad counts per brand")
    parser.add_argument("--force-refresh", action="store_true", help="Recompute even if column I already has a value")
    parser.add_argument("--limit", type=int, default=0, help="Optional max number of rows to process")
    parser.add_argument(
        "--no-sheet-write",
        action="store_true",
        help="Do not write to sheet; only print counts",
    )
    args = parser.parse_args()

    sheet = get_sheet_client()
    tasks = sheet_tasks(sheet, force_refresh=args.force_refresh)

    if args.limit and args.limit > 0:
        tasks = tasks[: args.limit]

    if not tasks:
        print("No rows need Meta ad count updates")
        return

    updates = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for row_number, brand_name in tasks:
            try:
                ad_count = get_meta_ad_count_for_brand(page, brand_name)
            except Exception as exc:
                print(f"Failed {brand_name}: {exc}")
                continue

            print(f"{brand_name} -> meta_ad_count={ad_count}")

            if not args.no_sheet_write:
                sheet.update_cell(row_number, AD_COUNT_COLUMN_INDEX, str(ad_count))
                updates += 1

        browser.close()

    if args.no_sheet_write:
        print("Completed without writing to sheet")
    else:
        print(f"Wrote {updates} Meta ad counts to column I")


if __name__ == "__main__":
    main()
