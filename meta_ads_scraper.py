import pandas as pd
import requests
import os
import re
import traceback
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright
from urllib.parse import urlparse

load_dotenv()

SEARCH_TERM = "free shipping"
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
BLOCKED_DOMAIN_HINTS = ["instagram", "facebook", "amazon", "tiktok"]
BLOCKED_DOMAIN_SUFFIXES = [".in", ".uk"]
JUNK_BRAND_WORDS = {"official", "store", "shop", "us", "inc"}
DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0"}
session = requests.Session()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "shopify_brands_meta")
BLACKLIST_WORKSHEET_NAME = os.getenv("BLACKLIST_WORKSHEET_NAME", "brand_blacklist")
GOOGLE_CREDS_FILE = os.getenv(
    "GOOGLE_CREDS_FILE", os.path.join(os.path.dirname(__file__), "creds.json")
)
SHEET_URL_COLUMN_INDEX = 2  # Column B


def normalize_url(url):
    if not url:
        return None

    normalized = url.strip().lower()
    normalized = normalized.replace("https://", "").replace("http://", "")
    normalized = normalized.replace("www.", "")
    return normalized.rstrip("/")


def normalize_brand_name(brand_name):
    if not brand_name:
        return None
    return " ".join(brand_name.strip().lower().split())


def is_blacklisted_url(url, blacklist_urls=None):
    normalized = normalize_url(url)
    if not normalized:
        return False

    if blacklist_urls and normalized in blacklist_urls:
        return True

    return any(normalized.endswith(suffix) for suffix in BLOCKED_DOMAIN_SUFFIXES)


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


def get_blacklist_urls():
    if not GOOGLE_SHEET_ID:
        return set()

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
        client = gspread.authorize(creds)
        worksheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(BLACKLIST_WORKSHEET_NAME)
    except Exception as exc:
        print(f"Unable to read blacklist worksheet '{BLACKLIST_WORKSHEET_NAME}': {exc}")
        return set()

    values = worksheet.get_all_values()
    blacklist_urls = set()

    for row in values:
        for cell in row:
            normalized = normalize_url(cell)
            if normalized:
                blacklist_urls.add(normalized)

    return blacklist_urls


def build_sheet_url_index(sheet):
    rows = sheet.get_all_values()
    url_index = {}

    for row_number, row in enumerate(rows, start=1):
        website_url = row[1] if len(row) > 1 else ""
        normalized_url = normalize_url(website_url)
        if not normalized_url or normalized_url in url_index:
            continue

        padded_row = (row + [""] * 5)[:5]
        url_index[normalized_url] = {
            "row_number": row_number,
            "values": padded_row,
        }

    return url_index, len(rows)


def build_sheet_brand_index(sheet):
    brand_index = set()
    for brand_name in sheet.col_values(1):
        normalized_brand = normalize_brand_name(brand_name)
        if normalized_brand:
            brand_index.add(normalized_brand)
    return brand_index


def _record_to_sheet_row(item):
    return [
        item.get("brand_name", ""),
        item.get("website_url") or item.get("url") or "",
    ]


def upsert_records_to_google_sheet(records, sheet, url_index, brand_index, next_row_number, blacklist_urls=None):
    if sheet is None or url_index is None or brand_index is None or not records:
        return next_row_number

    if blacklist_urls is None:
        blacklist_urls = set()

    rows_to_append = []
    append_keys = []
    row_updates = []

    for item in records:
        new_row = _record_to_sheet_row(item)
        normalized_url = normalize_url(new_row[1])
        if not normalized_url:
            continue
        if is_blacklisted_url(normalized_url, blacklist_urls):
            continue

        existing = url_index.get(normalized_url)
        if existing is None:
            normalized_brand = normalize_brand_name(item.get("brand_name"))
            if normalized_brand and normalized_brand in brand_index:
                continue
            rows_to_append.append(new_row)
            append_keys.append(normalized_url)
            continue

        row_number = existing["row_number"]
        row_updates.append(
            {
                "range": f"A{row_number}:B{row_number}",
                "values": [new_row],
            }
        )
        existing["values"][0] = new_row[0]
        existing["values"][1] = new_row[1]

    if row_updates:
        sheet.batch_update(row_updates)
        print(f"Updated {len(row_updates)} existing rows in Google Sheet")

    if rows_to_append:
        sheet.append_rows(rows_to_append)
        for idx, key in enumerate(append_keys):
            url_index[key] = {
                "row_number": next_row_number + idx + 1,
                "values": rows_to_append[idx],
            }
            appended_brand = normalize_brand_name(rows_to_append[idx][0])
            if appended_brand:
                brand_index.add(appended_brand)
        next_row_number += len(rows_to_append)
        print(f"Appended {len(rows_to_append)} new rows to Google Sheet")

    return next_row_number


def sync_to_google_sheet(df):
    if isinstance(df, pd.DataFrame):
        records = df.to_dict(orient="records")
    else:
        records = list(df)

    if not records:
        return

    try:
        sheet = get_sheet_client()
    except Exception as exc:
        print(f"Unable to connect to Google Sheet: {exc}")
        return

    url_index, next_row_number = build_sheet_url_index(sheet)
    brand_index = build_sheet_brand_index(sheet)
    blacklist_urls = get_blacklist_urls()
    upsert_records_to_google_sheet(
        records,
        sheet,
        url_index,
        brand_index,
        next_row_number,
        blacklist_urls,
    )


def init_google_sheet_sync():
    try:
        sheet = get_sheet_client()
    except Exception as exc:
        print(f"Unable to connect to Google Sheet: {exc}")
        return None, None, None, 0

    url_index, next_row_number = build_sheet_url_index(sheet)
    brand_index = build_sheet_brand_index(sheet)
    blacklist_urls = get_blacklist_urls()
    return sheet, url_index, brand_index, next_row_number, blacklist_urls


def sync_batch_to_google_sheet(records, sheet, url_index, brand_index, next_row_number, blacklist_urls):
    return upsert_records_to_google_sheet(
        records,
        sheet,
        url_index,
        brand_index,
        next_row_number,
        blacklist_urls,
    )


def extract_brand_from_text(text):
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    for j, line in enumerate(lines):
        if line == "Sponsored" and j > 0:
            candidate = lines[j - 1]
            if candidate != "See ad details":
                return candidate
    return None


def extract_domain(url):
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return None


def clean_brand_name(name):
    name = name.lower()
    if " with " in name:
        name = name.split(" with ")[0]
    return name.strip()


def normalize(text):
    return re.sub(r"[^a-z0-9]", "", text.lower())


def simplify_brand_for_domain(brand):
    lowered = brand.lower()
    tokens = re.findall(r"[a-z0-9]+", lowered)
    filtered_tokens = [token for token in tokens if token not in JUNK_BRAND_WORDS]
    return "".join(filtered_tokens)


def guess_domain(brand):
    cleaned = simplify_brand_for_domain(brand)
    if not cleaned:
        return None

    return f"{cleaned}.com"


def is_valid_domain(domain):
    try:
        response = session.get(f"https://{domain}", timeout=5, headers=DEFAULT_HEADERS)
        return response.status_code < 400
    except Exception:
        return False


def get_domain_serper(brand):
    if not SERPER_API_KEY:
        return None

    brand = clean_brand_name(brand)

    endpoint = "https://google.serper.dev/search"
    payload = {"q": f"{brand} official website", "num": 5}
    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json",
    }

    try:
        response = session.post(endpoint, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        results = response.json()
    except Exception:
        return None

    best_match = None
    brand_norm = normalize(brand)

    for result in results.get("organic", []):
        link = result.get("link", "")
        title = result.get("title", "")

        if not link:
            continue

        if any(hint in link.lower() for hint in BLOCKED_DOMAIN_HINTS):
            continue

        domain = extract_domain(link)
        if not domain:
            continue

        domain_norm = normalize(domain)
        title_norm = normalize(title)

        if brand_norm in domain_norm:
            return domain

        if domain_norm in brand_norm:
            return domain

        if brand_norm in title_norm:
            best_match = domain

    if best_match:
        return best_match

    for result in results.get("organic", []):
        link = result.get("link", "")
        if not link:
            continue
        if any(hint in link.lower() for hint in BLOCKED_DOMAIN_HINTS):
            continue
        return extract_domain(link)

    return None


def get_domain(brand):
    guessed = guess_domain(brand)
    if guessed and is_valid_domain(guessed):
        return guessed

    return get_domain_serper(brand)


def is_shopify_store(domain):
    try:
        res = session.get(f"https://{domain}", timeout=5, headers=DEFAULT_HEADERS)
        html = res.text.lower()

        shopify_signals = [
            "cdn.shopify.com",
            "myshopify.com",
            "shopify.theme",
            "shopify-payment-button",
        ]

        return any(signal in html for signal in shopify_signals)
    except Exception:
        return False


def process_brand(brand, seen_domains, seen_domains_lock, blacklist_urls):
    domain = get_domain(brand)
    if not domain:
        return None

    if is_blacklisted_url(domain, blacklist_urls):
        return None

    if not is_shopify_store(domain):
        return None

    with seen_domains_lock:
        if domain in seen_domains:
            return None
        seen_domains.add(domain)

    return {
        "brand_name": brand,
        "url": domain,
    }


def resolve_brands_parallel(brands, seen_domains, seen_domains_lock, blacklist_urls, max_workers=10):
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_brand, brand, seen_domains, seen_domains_lock, blacklist_urls)
            for brand in brands
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as exc:
                print("FULL ERROR:")
                traceback.print_exc()
                continue

    return results


def scrape_ads():
    discovered_brands = set()
    processed_brands = set()
    seen_domains = set()
    seen_domains_lock = Lock()
    discovered_brand_data = {}
    sheet, url_index, brand_index, next_row_number, blacklist_urls = init_google_sheet_sync()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=US&search_type=keyword_unordered&q={SEARCH_TERM}"
        page.goto(url)

        page.wait_for_timeout(5000)

        prev_count = 0
        while True:
            ads = page.locator("div:has-text('See ad details')")
            curr_count = ads.count()

            for i in range(curr_count):
                try:
                    text = ads.nth(i).inner_text(timeout=1200)
                    if "Library ID" not in text:
                        continue

                    brand_name = extract_brand_from_text(text)
                    if not brand_name:
                        ad = ads.nth(i)
                        brand_locator = ad.locator("span, strong")
                        if brand_locator.count() > 0:
                            brand_name = brand_locator.first.inner_text(timeout=1000).strip()

                    if not brand_name:
                        continue

                    normalized_brand = normalize_brand_name(brand_name)
                    if brand_index and normalized_brand in brand_index:
                        continue

                    discovered_brands.add(brand_name)
                except Exception as exc:
                    print("FULL ERROR:")
                    traceback.print_exc()
                    continue

            candidate_brands = sorted(discovered_brands - processed_brands)
            new_brands = []
            for brand_name in candidate_brands:
                normalized_brand = normalize_brand_name(brand_name)
                if brand_index and normalized_brand in brand_index:
                    continue
                new_brands.append(brand_name)

            processed_brands.update(candidate_brands)
            if new_brands:
                resolved_batch = resolve_brands_parallel(
                    new_brands,
                    seen_domains,
                    seen_domains_lock,
                    blacklist_urls,
                    max_workers=10,
                )

                for item in resolved_batch:
                    discovered_brand_data[item["brand_name"]] = {"url": item["url"]}

                next_row_number = sync_batch_to_google_sheet(
                    resolved_batch,
                    sheet,
                    url_index,
                    brand_index,
                    next_row_number,
                    blacklist_urls,
                )

            print(f"Loaded ads: {curr_count}")
            print(f"Total unique brands with URLs: {len(discovered_brand_data)}")
            for brand_name, payload in discovered_brand_data.items():
                print(f"{brand_name} | {payload['url']}")

            if curr_count == prev_count:
                break

            prev_count = curr_count

            page.mouse.wheel(0, 8000)
            page.wait_for_timeout(2000)

        browser.close()

    return pd.DataFrame(
        [
            {
                "brand_name": brand_name,
                "url": payload["url"],
            }
            for brand_name, payload in discovered_brand_data.items()
        ],
        columns=["brand_name", "url"],
    )


data = scrape_ads()
data = data.drop_duplicates(subset=["brand_name"]) 
print(
    data[
        [
            "brand_name",
            "url",
        ]
    ]
)