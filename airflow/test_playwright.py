from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser=p.chromium.launch(
        headless=True,  #chromium runs without any visible windows False also doesn/t egenrate GUI since Docker containers don't have a display server.
        args=["--no-sandbox", "--disable-dev-shm-usage"]
    )

    page=browser.new_page()
    page.goto("https://www.smartprix.com/mobiles", wait_until='domcontentloaded', timeout=60000)
    print("Page title: ", page.title())
    browser.close()

print("SUCCESS-Chromiumn launched and loaded a page inside the container")