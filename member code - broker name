import pandas as pd
from playwright.sync_api import sync_playwright


def scrape_all_brokers():
    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page(viewport={"width": 1600, "height": 1200})

        page.goto(
            "https://www.bseindia.com/members/MembershipDirectory.aspx",
            wait_until="domcontentloaded",
            timeout=60000
        )
        page.wait_for_timeout(5000)

        # Wait for table to appear
        page.wait_for_selector("table")
        page.wait_for_timeout(3000)

        visited_pages = set()

        while True:
            page_no = page.locator("a.current, span.current").first.text_content(timeout=3000) if page.locator("a.current, span.current").count() > 0 else None
            if page_no in visited_pages:
                break
            if page_no:
                visited_pages.add(page_no)

            tables = page.locator("table")
            table_found = False

            for t in range(tables.count()):
                table = tables.nth(t)
                rows = table.locator("tr")

                if rows.count() < 2:
                    continue

                headers = [h.inner_text().strip() for h in rows.nth(0).locator("th, td").all()]
                normalized_headers = [h.lower().replace("\n", " ").strip() for h in headers]

                if any("member name" in h for h in normalized_headers) and any("member code" in h for h in normalized_headers):
                    table_found = True

                    for i in range(1, rows.count()):
                        cols = [c.inner_text().strip() for c in rows.nth(i).locator("td").all()]
                        if len(cols) >= 5:
                            all_rows.append({
                                "Sr No.": cols[0],
                                "Member Name": cols[1],
                                "Trade Name": cols[2],
                                "Member Code": cols[3],
                                "SEBI Registration No.": cols[4]
                            })
                    break

            if not table_found:
                print("No valid data table found on this page.")
                break

            # Try clicking Next page if available
            next_button = page.locator("text=Next").first

            if next_button.count() == 0 or not next_button.is_visible():
                break

            try:
                next_button.click()
                page.wait_for_timeout(4000)
            except Exception:
                break

        browser.close()

    df = pd.DataFrame(all_rows).drop_duplicates()
    return df


if __name__ == "__main__":
    df = scrape_all_brokers()
    df.to_excel("bse_all_brokers.xlsx", index=False)
    print(df)
