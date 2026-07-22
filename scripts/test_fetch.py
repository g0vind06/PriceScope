# import requests
# from config.settings import HEADERS

# url='https://www.croma.com/phones-wearables/c/1'
# response = requests.get(url, headers=HEADERS, timeout=15)

# #search for key selector in the raw HTML
# if 'product-item' in response.text:
#     print("Key selector found in the HTML.")
# else:
#     print("Key selector not found in the HTML, site likely needs JavaScript")
#     print(f"Staus Code: {response.status_code}")
#     print(f"Response Text: {response.text[:500]}")  # Print the first 500 characters of the response text for debugging

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time

def test_smartprix():
    print("Launching browser...")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False # Show browser window — bypasses basic detection
        )

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            locale="en-IN",
            viewport={"width": 1920, "height": 1080}
        )

        page = context.new_page()
        url = "https://www.smartprix.com/mobiles"
        print(f"Navigating to {url}")
        page.goto(url, wait_until="networkidle", timeout=30000)

        print("Waiting for render...")
        time.sleep(5)

        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, 'lxml')

    # Check what's in the page
    print(f"\nPage title: {soup.title.string if soup.title else 'No title'}")
    print(f"Total divs found: {len(soup.find_all('div'))}")

    # Save for inspection
    with open("debug_output.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("Saved to debug_output.html")
    print("\nOpen debug_output.html and Ctrl+F search for a product name you see on smartprix.com")

if __name__ == "__main__":
    test_smartprix()