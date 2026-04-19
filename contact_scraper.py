import argparse
import os
import time
from urllib.parse import urlparse

import gspread
import requests
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
BRANDS_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "shopify_brands_meta")
LEADS_WORKSHEET_NAME = "leads"
GOOGLE_CREDS_FILE = os.getenv(
    "GOOGLE_CREDS_FILE", os.path.join(os.path.dirname(__file__), "creds.json")
)
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")

# shopify_brands_meta columns (1-based)
URL_COLUMN_INDEX = 2   # Column B
BRAND_COLUMN_INDEX = 1  # Column A

# leads sheet columns (1-based): name | company | role | email | linkedin
LEADS_HEADERS = ["name", "company", "role", "email", "linkedin"]

APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_ENRICH_URL = "https://api.apollo.io/api/v1/people/match"

MAX_CONTACTS_PER_DOMAIN = 5
REQUEST_DELAY_SECONDS = 1.0  # Apollo rate limit buffer


def get_gspread_client():
    if not GOOGLE_SHEET_ID:
        raise ValueError("Missing GOOGLE_SHEET_ID environment variable")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
    return gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)


def get_brands_sheet(workbook):
    return workbook.worksheet(BRANDS_WORKSHEET_NAME)


def get_leads_sheet(workbook):
    """Return the leads worksheet, creating it with headers if it doesn't exist."""
    try:
        sheet = workbook.worksheet(LEADS_WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        sheet = workbook.add_worksheet(title=LEADS_WORKSHEET_NAME, rows=1000, cols=len(LEADS_HEADERS))
        sheet.append_row(LEADS_HEADERS)
        print(f"Created '{LEADS_WORKSHEET_NAME}' worksheet with headers")
    return sheet


def build_leads_email_index(leads_sheet):
    """Return a set of emails already in the leads sheet (for dedup)."""
    email_col = LEADS_HEADERS.index("email") + 1
    existing = leads_sheet.col_values(email_col)
    return {e.strip().lower() for e in existing if e.strip()}


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


def apollo_search_contacts(domain):
    """Search Apollo for people at a domain. Returns person stubs with IDs (no emails)."""
    if not APOLLO_API_KEY:
        raise ValueError("Missing APOLLO_API_KEY environment variable")

    headers = {"X-Api-Key": APOLLO_API_KEY}
    params = {
        "q_organization_domains_list[]": domain,
        "per_page": MAX_CONTACTS_PER_DOMAIN,
        "page": 1,
    }

    response = requests.post(APOLLO_SEARCH_URL, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    return response.json().get("people", [])


def apollo_enrich_person(person_id):
    """Enrich a person by Apollo ID to retrieve their full name, email, and LinkedIn URL."""
    headers = {"X-Api-Key": APOLLO_API_KEY}
    params = {"id": person_id}

    response = requests.post(APOLLO_ENRICH_URL, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    return response.json().get("person", {})


def person_to_leads_row(person, company_name):
    """Map an Apollo person record to a leads sheet row."""
    first = (person.get("first_name") or "").strip()
    last = (person.get("last_name") or person.get("last_name_obfuscated") or "").strip()
    name = f"{first} {last}".strip()

    role = (person.get("title") or "").strip()
    email = (person.get("email") or "").strip()
    linkedin = (person.get("linkedin_url") or "").strip()

    org = company_name
    if not org:
        org_data = person.get("organization") or {}
        org = (org_data.get("name") or "").strip()

    return [name, org, role, email, linkedin]


def fetch_contacts_for_domain(domain, company_name):
    """Search for contacts at a domain, then enrich those with emails to get full data."""
    try:
        people = apollo_search_contacts(domain)
    except requests.HTTPError as exc:
        print(f"  Apollo HTTP {exc.response.status_code} for {domain}: {exc.response.text}")
        return []
    except Exception as exc:
        print(f"  Apollo error for {domain}: {exc}")
        return []

    rows = []
    for person in people:
        if person.get("has_email") and person.get("id"):
            try:
                enriched = apollo_enrich_person(person["id"])
                if enriched:
                    person = {**person, **enriched}
            except Exception as exc:
                print(f"  Enrichment failed for {person.get('id')}: {exc}")

        rows.append(person_to_leads_row(person, company_name))

    return rows


def brands_tasks(brands_sheet):
    """Return list of (domain, company_name) from the brands sheet."""
    rows = brands_sheet.get_all_values()
    tasks = []

    for row_number, row in enumerate(rows, start=1):
        if row_number == 1:
            continue  # skip header

        company_name = row[BRAND_COLUMN_INDEX - 1].strip() if len(row) >= BRAND_COLUMN_INDEX else ""
        website_url = row[URL_COLUMN_INDEX - 1].strip() if len(row) >= URL_COLUMN_INDEX else ""

        if not website_url:
            continue

        domain = extract_domain(website_url)
        if not domain:
            continue

        tasks.append((domain, company_name))

    return tasks


def main():
    parser = argparse.ArgumentParser(
        description="Find contacts for each brand domain via Apollo.io and write to the leads sheet"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of domains to process",
    )
    parser.add_argument(
        "--no-sheet-write",
        action="store_true",
        help="Print results without writing to the leads sheet",
    )
    parser.add_argument(
        "--domain",
        dest="single_domain",
        help="Look up a single domain instead of reading from the brands sheet",
    )
    args = parser.parse_args()

    workbook = get_gspread_client()
    leads_sheet = get_leads_sheet(workbook)
    existing_emails = build_leads_email_index(leads_sheet)

    if args.single_domain:
        domain = extract_domain(args.single_domain) or args.single_domain
        rows = fetch_contacts_for_domain(domain, company_name="")
        for row in rows:
            print(row)
        return

    brands_sheet = get_brands_sheet(workbook)
    tasks = brands_tasks(brands_sheet)

    if args.limit and args.limit > 0:
        tasks = tasks[: args.limit]

    if not tasks:
        print("No domains found in brands sheet")
        return

    total_written = 0
    for domain, company_name in tasks:
        rows = fetch_contacts_for_domain(domain, company_name)

        new_rows = []
        for row in rows:
            email = row[LEADS_HEADERS.index("email")].lower()
            if email and email in existing_emails:
                print(f"  Skipping duplicate: {email}")
                continue
            new_rows.append(row)
            if email:
                existing_emails.add(email)

        if new_rows:
            print(f"  {domain} -> {len(new_rows)} contact(s)")
            for row in new_rows:
                print(f"    {row}")
            if not args.no_sheet_write:
                leads_sheet.append_rows(new_rows)
                total_written += len(new_rows)
        else:
            print(f"  {domain} -> no new contacts")

        time.sleep(REQUEST_DELAY_SECONDS)

    if args.no_sheet_write:
        print("Completed without writing to sheet")
    else:
        print(f"Wrote {total_written} contact(s) to '{LEADS_WORKSHEET_NAME}' sheet")


if __name__ == "__main__":
    main()
