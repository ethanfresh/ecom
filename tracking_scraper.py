import os
import re
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright


GOOGLE_SHEET_ID = "1slQi497BwFy-6FR72mU_7dSjyPlatYM7R6_7NqPDLxw"
GOOGLE_WORKSHEET_NAME = "shopify_brands_meta"
GOOGLE_CREDS_FILE = os.path.join(os.path.dirname(__file__), "creds.json")

# Sheet columns (1-based):
# A brand_name | B url | C tracking_score | D tracking_stack | E tracking_quality
URL_COLUMN_INDEX = 2
TRACKING_SCORE_COLUMN_INDEX = 3
TRACKING_STACK_COLUMN_INDEX = 4
TRACKING_QUALITY_COLUMN_INDEX = 5

FORCE_REFRESH = True
MAX_WORKERS = 4


def get_sheet_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_WORKSHEET_NAME)


def normalize_url(url):
    if not url:
        return ""
    return (
        url.strip()
        .lower()
        .replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .rstrip("/")
    )


def extract_domain(url):
    if not url:
        return None

    value = url.strip()
    if not value.startswith("http://") and not value.startswith("https://"):
        value = f"https://{value}"

    try:
        parsed = urlparse(value)
        return parsed.netloc.replace("www.", "").lower()
    except Exception:
        return None


def fetch_site_html_rendered(domain):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            page.goto(f"https://{domain}", timeout=10000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
            html = page.content()

            # Product pages often load conversion/event scripts not visible on homepage.
            try:
                product_links = page.locator("a[href*='/products/']")
                if product_links.count() > 0:
                    href = product_links.first.get_attribute("href")
                    if href:
                        product_url = urljoin(f"https://{domain}", href)
                        page.goto(product_url, timeout=10000, wait_until="domcontentloaded")
                        page.wait_for_timeout(2000)
                        html += page.content()
            except Exception:
                pass

            browser.close()
            return html
    except Exception:
        return None


def score_tracking_html(html):
    if not html:
        return 0, [], "unknown"

    h = html.lower()
    score = 0.0
    stack = []

    has_gtm = "googletagmanager" in h
    gtm_ids = re.findall(r"GTM-[A-Z0-9]+", h)

    has_meta_direct = any(
        x in h
        for x in [
            "fbq(",
            "facebook.com/tr",
            "connect.facebook.net",
            "fbevents.js",
            "fbq.push",
            "fbq.callmethod",
            "meta pixel",
        ]
    )
    has_meta_inferred = False

    if has_meta_direct:
        score += 2
        stack.append("Meta (direct)")
    elif has_gtm:
        score += 0.5
        has_meta_inferred = True
        stack.append("Meta (via GTM - inferred)")
    else:
        stack.append("Meta (not detected)")

    has_meta_capi = any(
        x in h
        for x in [
            "graph.facebook.com",
            "server_side_api",
            "event_id",
            "fbp",
            "fbc",
        ]
    )
    if has_meta_capi:
        score += 1
        stack.append("Meta CAPI (signal)")

    has_ga = any(x in h for x in ["gtag(", "google-analytics.com", "analytics.js", "gtag/js"])

    if has_gtm:
        score += 1
        if gtm_ids:
            stack.append(f"GTM ({gtm_ids[0]})")
        else:
            stack.append("GTM")

    if has_ga:
        score += 1
        stack.append("GA")

    email_types = []
    if "klaviyo" in h or "_learnq" in h:
        email_types.append("Klaviyo")
    if "mailchimp" in h:
        email_types.append("Mailchimp")
    if "attentive" in h:
        email_types.append("Attentive")
    if "postscript" in h:
        email_types.append("Postscript")

    if email_types:
        score += 1
        stack.extend(email_types)

    has_tiktok = any(x in h for x in ["analytics.tiktok.com", "ttq.load", "tiktok pixel"])
    if has_tiktok:
        score += 0.5
        stack.append("TikTok")

    if "trekkie.storefront" in h or "shopify-analytics" in h:
        score += 0.5
        stack.append("Shopify Analytics")

    if "segment.com" in h or "analytics.segment" in h:
        score += 1
        stack.append("Segment")

    if "triplewhale" in h:
        score += 1
        stack.append("Triple Whale")

    if "northbeam" in h:
        score += 1
        stack.append("Northbeam")

    if "hotjar" in h or "hj(" in h:
        stack.append("Hotjar")

    stack = list(dict.fromkeys(stack))

    if score >= 6:
        quality = "strong"
    elif score >= 4:
        quality = "mid"
    elif score >= 2:
        quality = "low-mid"
    else:
        quality = "weak"

    return score, stack, quality


def process_row(row_number, website_url):
    domain = extract_domain(website_url)
    if not domain:
        return row_number, None

    html = fetch_site_html_rendered(domain)
    if not html:
        return row_number, None

    score, stack, quality = score_tracking_html(html)
    return row_number, {
        "score": score,
        "stack": ", ".join(stack),
        "quality": quality,
        "url": normalize_url(website_url),
    }


def main():
    sheet = get_sheet_client()
    rows = sheet.get_all_values()

    tasks = []
    for row_number, row in enumerate(rows, start=1):
        if row_number == 1:
            continue

        website_url = row[URL_COLUMN_INDEX - 1] if len(row) >= URL_COLUMN_INDEX else ""
        existing_score = row[TRACKING_SCORE_COLUMN_INDEX - 1] if len(row) >= TRACKING_SCORE_COLUMN_INDEX else ""
        existing_stack = row[TRACKING_STACK_COLUMN_INDEX - 1] if len(row) >= TRACKING_STACK_COLUMN_INDEX else ""
        existing_quality = row[TRACKING_QUALITY_COLUMN_INDEX - 1] if len(row) >= TRACKING_QUALITY_COLUMN_INDEX else ""

        if not website_url:
            continue

        if not FORCE_REFRESH and existing_score and existing_stack and existing_quality:
            continue

        tasks.append((row_number, website_url))

    if not tasks:
        print("No rows need tracking updates")
        return

    updates = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(process_row, row_number, website_url): (row_number, website_url)
            for row_number, website_url in tasks
        }

        for future in as_completed(future_map):
            row_number, website_url = future_map[future]
            try:
                _, result = future.result()
            except Exception:
                print("FULL ERROR:")
                traceback.print_exc()
                continue

            if not result:
                continue

            sheet.update(
                f"C{row_number}:E{row_number}",
                [[result["score"], result["stack"], result["quality"]]],
            )
            updates += 1
            print(
                f"Updated {result['url']} -> score={result['score']} | stack={result['stack']} | quality={result['quality']}"
            )

    print(f"Wrote tracking updates for {updates} rows")


if __name__ == "__main__":
    main()
