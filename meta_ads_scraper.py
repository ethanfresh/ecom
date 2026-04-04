from playwright.sync_api import sync_playwright
import time

SEARCH_TERM = "free shipping"

def scrape_ads():
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=US&search_type=keyword_unordered&q={SEARCH_TERM}"
        page.goto(url)

        # WAIT FOR CONTENT
        page.wait_for_timeout(8000)

        # SCROLL
        for _ in range(6):
            page.mouse.wheel(0, 5000)
            page.wait_for_timeout(2000)

        # DEBUG: print page content size
        print("Page loaded")

        # Try broader selector
        ads = page.locator("div:has-text('See ad details')")

        count = ads.count()
        print(f"Found {count} ads")

        results = []

        for i in range(min(ads.count(), 20)):
            try:
                ad = ads.nth(i)

                brand_el = ad.locator("a[href*='/ads/library/?active_status']").first
                brand = brand_el.inner_text()

                text = ad.inner_text(timeout=5000)

                status = "ACTIVE" if "Active" in text else "INACTIVE"

                if "Library ID" not in text:
                    continue  # skip non-ads

                print("\n--- REAL AD ---")
                print(text[:200])

                # Extract advertiser name (usually first meaningful line)
                lines = text.split("\n")
                brand = None

                for line in lines:
                    if line and "Library ID" not in line and "Sponsored" not in line:
                        brand = line
                        break

                # Extract outbound links
                links = ad.locator("a").all()
                website = None

                for l in links:
                    href = l.get_attribute("href")
                    if href and "http" in href and "facebook" not in href:
                        website = href
                        break

                results.append({
                    "brand": brand,
                    "website": website
                })

            except Exception as e:
                print("Error:", e)
                continue

        browser.close()

    return results


data = scrape_ads()
print(f"\nCollected {len(data)} ads")