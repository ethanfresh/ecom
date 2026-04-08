import argparse
import os
import re
from urllib.parse import urljoin, urlparse

import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials


GOOGLE_SHEET_ID = "1slQi497BwFy-6FR72mU_7dSjyPlatYM7R6_7NqPDLxw"
GOOGLE_WORKSHEET_NAME = "shopify_brands_meta"
GOOGLE_CREDS_FILE = os.path.join(os.path.dirname(__file__), "creds.json")
URL_COLUMN_INDEX = 2
SUBSCRIPTION_MODEL_COLUMN_INDEX = 8  # Column H

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

session = requests.Session()
session.headers.update(DEFAULT_HEADERS)


SUBSCRIPTION_KEYWORDS_STRONG = [
    "subscribe and save",
    "subscription",
    "recurring delivery",
    "delivery every",
    "auto-renew",
    "membership",
    "member pricing",
    "monthly box",
    "weekly box",
    "join the club",
    "cancel anytime",
]

SUBSCRIPTION_APPS = [
    "rechargepayments",
    "rechargecdn",
    "skio",
    "stay.ai",
    "loop-subscriptions",
    "seal-subscriptions",
    "appstle",
    "bold-subscriptions",
    "smartrr",
]

SUBSCRIPTION_LINK_HINTS = [
    "subscription",
    "subscribe",
    "membership",
    "memberships",
    "plans",
    "club",
    "box",
    "refill",
]

NEWSLETTER_ONLY_HINTS = [
    "subscribe to our newsletter",
    "newsletter",
    "email subscription",
]


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


def get_sheet_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_WORKSHEET_NAME)


def fetch_html(url):
    try:
        response = session.get(url, timeout=10)
        if response.status_code >= 400:
            return None
        return response.text
    except Exception:
        return None


def strip_html(html):
    if not html:
        return ""
    no_script = re.sub(r"<script[\\s\\S]*?</script>", " ", html, flags=re.IGNORECASE)
    no_style = re.sub(r"<style[\\s\\S]*?</style>", " ", no_script, flags=re.IGNORECASE)
    no_tags = re.sub(r"<[^>]+>", " ", no_style)
    collapsed = re.sub(r"\\s+", " ", no_tags)
    return collapsed.lower()


def extract_candidate_links(base_url, html):
    if not html:
        return []

    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    candidates = []
    seen = set()

    for href in hrefs:
        href_lower = href.lower()
        if href_lower.startswith("javascript:") or href_lower.startswith("mailto:"):
            continue

        if not any(hint in href_lower for hint in SUBSCRIPTION_LINK_HINTS):
            continue

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if not parsed.netloc:
            continue

        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        if normalized in seen:
            continue

        seen.add(normalized)
        candidates.append(normalized)

    return candidates[:3]


def score_subscription_signals(url, html):
    score = 0
    evidence = []

    if not html:
        return score, evidence

    h = html.lower()
    text = strip_html(h)

    for app in SUBSCRIPTION_APPS:
        if app in h:
            score += 3
            evidence.append(f"Subscription app detected: {app}")

    for kw in SUBSCRIPTION_KEYWORDS_STRONG:
        if kw in text:
            score += 2
            evidence.append(f"Strong keyword: {kw}")

    # Reduce false positives from newsletter-only pages.
    if any(hint in text for hint in NEWSLETTER_ONLY_HINTS):
        score -= 1
        evidence.append("Newsletter-only subscription language found")

    if any(part in url.lower() for part in SUBSCRIPTION_LINK_HINTS):
        score += 1
        evidence.append("Subscription-oriented URL path")

    return max(score, 0), list(dict.fromkeys(evidence))


def classify_subscription(score):
    if score >= 6:
        return "likely"
    if score >= 3:
        return "possible"
    return "unlikely"


def analyze_domain(domain):
    homepage_url = f"https://{domain}"
    homepage_html = fetch_html(homepage_url)
    if not homepage_html:
        return {
            "url": domain,
            "subscription_model": "unknown",
            "subscription_score": 0,
            "evidence": "Homepage fetch failed",
        }

    total_score, evidence = score_subscription_signals(homepage_url, homepage_html)

    for candidate_url in extract_candidate_links(homepage_url, homepage_html):
        linked_html = fetch_html(candidate_url)
        link_score, link_evidence = score_subscription_signals(candidate_url, linked_html)
        total_score += link_score
        evidence.extend(link_evidence)

    evidence = list(dict.fromkeys(evidence))

    return {
        "url": domain,
        "subscription_model": classify_subscription(total_score),
        "subscription_score": total_score,
        "evidence": " | ".join(evidence[:8]) if evidence else "No subscription signals detected",
    }


def sheet_tasks(sheet):
    rows = sheet.get_all_values()
    tasks = []

    for row_number, row in enumerate(rows, start=1):
        if row_number == 1:
            continue

        website_url = row[URL_COLUMN_INDEX - 1] if len(row) >= URL_COLUMN_INDEX else ""
        domain = extract_domain(website_url)
        if not domain:
            continue

        tasks.append((row_number, domain))

    return tasks


def write_results_to_sheet(sheet, row_results):
    if not row_results:
        return 0

    updates = []
    for row_number, result in row_results:
        updates.append(
            {
                "range": f"H{row_number}:H{row_number}",
                "values": [[result["subscription_model"]]],
            }
        )

    if updates:
        sheet.batch_update(updates)
    return len(updates)


def write_single_result_to_sheet(sheet, row_number, result):
    sheet.update_cell(row_number, SUBSCRIPTION_MODEL_COLUMN_INDEX, result["subscription_model"])


def main():
    parser = argparse.ArgumentParser(
        description="Detect whether websites appear to offer a subscription model"
    )
    parser.add_argument(
        "--url",
        dest="single_url",
        help="Analyze a single URL/domain instead of reading from Google Sheet",
    )
    args = parser.parse_args()

    if args.single_url:
        domain = extract_domain(args.single_url)
        if not domain:
            print("Invalid URL")
            return
        result = analyze_domain(domain)
        print(result)
        return

    sheet = get_sheet_client()
    tasks = sheet_tasks(sheet)
    if not tasks:
        print("No valid URLs found in sheet")
        return

    write_count = 0
    for row_number, domain in tasks:
        result = analyze_domain(domain)
        write_single_result_to_sheet(sheet, row_number, result)
        write_count += 1
        print(
            f"Updated row {row_number} ({result['url']}) -> model={result['subscription_model']} score={result['subscription_score']}"
        )

    print(f"Wrote {write_count} subscription model values to column H")


if __name__ == "__main__":
    main()
