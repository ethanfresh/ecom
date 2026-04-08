import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import urlparse

import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials


GOOGLE_SHEET_ID = "1slQi497BwFy-6FR72mU_7dSjyPlatYM7R6_7NqPDLxw"
GOOGLE_WORKSHEET_NAME = "shopify_brands_meta"
GOOGLE_CREDS_FILE = os.path.join(os.path.dirname(__file__), "creds.json")

# Sheet columns (1-based):
# A brand_name | B website_url | C tracking_score | D tracking_stack | E tracking_quality | F product_count
URL_COLUMN_INDEX = 2
PRODUCT_COUNT_COLUMN_INDEX = 6
MAX_WORKERS = 1

session = requests.Session()
session.headers.update(
	{"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
)
request_lock = Lock()
last_request_time = [0.0]


def throttled_get(url):
	with request_lock:
		now = time.time()
		wait_seconds = max(0.0, 1.5 - (now - last_request_time[0]))
		if wait_seconds > 0:
			time.sleep(wait_seconds)
		last_request_time[0] = time.time()

	return session.get(url, timeout=5)


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


def get_product_count(domain):
	# Fast accessibility check before paginating.
	test_url = f"https://{domain}/products.json?limit=1"
	try:
		test_res = throttled_get(test_url)
		if test_res.status_code == 429:
			print(f"{domain} failed with status 429 (cooldown + retry)")
			time.sleep(10 + random.uniform(0, 5))
			test_res = throttled_get(test_url)
		if test_res.status_code != 200:
			print(f"{domain} failed with status {test_res.status_code}")
			return None
	except Exception as e:
		print(f"{domain} exception: {e}")
		return None

	total = 0
	page = 1

	try:
		while True:
			url = f"https://{domain}/products.json?limit=250&page={page}"
			res = throttled_get(url)

			if res.status_code == 429:
				print(f"{domain} failed with status 429 (cooldown + retry)")
				time.sleep(10 + random.uniform(0, 5))
				continue

			if res.status_code >= 400:
				print(f"{domain} failed with status {res.status_code}")
				return None

			products = res.json().get("products", [])
			if not products:
				break

			total += len(products)
			page += 1

		return total
	except Exception as e:
		print(f"{domain} exception: {e}")
		return None


def process_row(row_number, website_url):
	domain = extract_domain(website_url)
	if not domain:
		return row_number, None

	time.sleep(random.uniform(2, 4))

	count = get_product_count(domain)
	return row_number, count


def main():
	sheet = get_sheet_client()
	rows = sheet.get_all_values()

	tasks = []
	for row_number, row in enumerate(rows, start=1):
		if row_number == 1:
			continue

		website_url = row[URL_COLUMN_INDEX - 1] if len(row) >= URL_COLUMN_INDEX else ""
		existing_count = row[PRODUCT_COUNT_COLUMN_INDEX - 1] if len(row) >= PRODUCT_COUNT_COLUMN_INDEX else ""

		if not website_url:
			continue

		# Skip rows where product count already exists.
		if existing_count and existing_count.strip():
			continue

		tasks.append((row_number, website_url))

	if not tasks:
		print("No rows need product count updates")
		return

	written_count = 0
	with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
		future_map = {
			executor.submit(process_row, row_number, website_url): (row_number, website_url)
			for row_number, website_url in tasks
		}

		for future in as_completed(future_map):
			row_number, website_url = future_map[future]
			try:
				_, count = future.result()
			except Exception:
				count = None

			if count is None:
				continue

			sheet.update_cell(row_number, PRODUCT_COUNT_COLUMN_INDEX, str(count))
			written_count += 1
			print(f"Updated {normalize_url(website_url)} -> product_count={count}")

	if written_count == 0:
		print("No product counts found to write")
		return

	print(f"Wrote {written_count} product counts to Google Sheets")


if __name__ == "__main__":
	main()
