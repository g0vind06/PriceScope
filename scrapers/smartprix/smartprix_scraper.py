import sys
import os
import json
import time
import random
import boto3
import argparse
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config.settings import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    AWS_REGION, S3_BUCKET_NAME, BRONZE_PREFIX
)

# --- CONFIGURATION ---
CATEGORIES = {
    "smartphones": "https://www.smartprix.com/mobiles",
    "laptops": "https://www.smartprix.com/laptops",
}
MAX_PAGES = 2 # 2 pages × ~20 products = ~40 products per category per run


# --- PRICE PARSER ---
def parse_price(text):
    """'₹97,999' → 97999.0"""
    if not text:
        return None
    try:
        return float(text.replace('₹', '').replace(',', '').strip())
    except ValueError:
        return None


def parse_discount(text):
    """'7%' → 7"""
    if not text:
        return None
    try:
        return int(text.replace('%', '').strip())
    except ValueError:
        return None


def parse_rating(text):
    """'4.15' → 4.15"""
    if not text:
        return None
    try:
        return float(text.strip())
    except ValueError:
        return None


# --- PAGE FETCHER ---
def fetch_page(page, url, page_num):
    """
    Navigate to URL with pagination.
    Smartprix uses ?page=N for pagination.
    """
    full_url = f"{url}?page={page_num}" if page_num > 1 else url
    print(f" Fetching: {full_url}")

    page.goto(full_url, wait_until="domcontentloaded", timeout=60000)
    #Wait for the product cards to load
    page.wait_for_selector('div.sm-product', timeout=15000)

    # Random human-like delay
    time.sleep(random.uniform(3, 6))

    return page.content()


# --- PARSER ---
def parse_products(html, category):
    """Extract all product cards from one page."""
    soup = BeautifulSoup(html, 'lxml')

    # Product cards
    cards = soup.find_all('div', class_='sm-product')

    if not cards:
        print(f" No product cards found on this page.")
        return []

    products = []
    scraped_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()  #(datetime.now(timezone.utc).replace(tzinfo=None).isoformat() will give you a string representation of the current UTC time in ISO 8601 format, which is widely used and easily readable.)

    for card in cards:
        try:
            # --- Name + URL ---
            name_tag = card.find('a', class_=lambda c: c and 'name' in c)
            if not name_tag:
                continue

            name = name_tag.get_text(strip=True)
            relative_url = name_tag.get('href', '')
            product_url = (
                f"https://www.smartprix.com{relative_url}"
                if relative_url.startswith('/')
                else relative_url
            )

            # --- Price ---
            price_tag = card.find('span', class_='price')
            current_price = parse_price(
                price_tag.get_text(strip=True) if price_tag else None
            )

            # --- Discount ---
            discount_tag = card.find('span', class_='sm-pdrop')
            discount_pct = parse_discount(
                discount_tag.get_text(strip=True) if discount_tag else None
            )

            # --- Rating ---
            rating_tag = card.find('span', class_='sm-rating')
            rating = parse_rating(
                rating_tag.get_text(strip=True) if rating_tag else None
            )

            # --- Skip cards without critical fields ---
            if not name or not current_price:
                continue

            # --- Compute MRP from discount ---
            # If discount is known: MRP = price / (1 - discount/100)
            mrp = None
            if current_price and discount_pct:
                try:
                    mrp = round(current_price / (1 - discount_pct / 100), 2)
                except ZeroDivisionError:
                    mrp = None

            products.append({
                'product_name': name,
                'product_url': product_url,
                'current_price': current_price,
                'mrp': mrp,
                'discount_pct': discount_pct,
                'rating': rating,
                'category': category,
                'source': 'smartprix',
                'scraped_at': scraped_at,
            })

        except Exception as e:
            print(f" Skipped one card: {e}")
            continue

    return products


# --- S3 UPLOAD ---
def upload_to_s3(data, category, partition_dt=None):
    """
    Upload JSON to:
    bronze/source=smartprix/category={cat}/year=/month=/day=/hour=/file.json

    partition_dt sets the s3 partition folder (aligned to Airflow's logical run). The filename itself uses real upload time for traceability.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    partition = partition_dt if partition_dt else datetime.now(timezone.utc).replace(tzinfo=None)
    upload_time = datetime.now(timezone.utc).replace(tzinfo=None)  # Actual upload time for filename

    now = datetime.now(timezone.utc).replace(tzinfo=None)  # UTC without tzinfo for S3 key
    s3_key = (
        f"{BRONZE_PREFIX}/source=smartprix/category={category}/"
        f"year={partition.year}/month={partition.month:02d}/"
        f"day={partition.day:02d}/hour={partition.hour:02d}/"
        f"smartprix_{category}_{upload_time.strftime('%Y%m%d_%H%M%S')}.json"
    )

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data, ensure_ascii=False, indent=2),
        ContentType='application/json'
    )

    print(f" Uploaded → s3://{S3_BUCKET_NAME}/{s3_key}")
    return s3_key


# --- MAIN ---
def scrape_category(playwright_page, category_name, base_url):
    all_products = []

    for page_num in range(1, MAX_PAGES + 1):
        print(f"\n Page {page_num}...")
        html = fetch_page(playwright_page, base_url, page_num)
        products = parse_products(html, category_name)

        if not products:
            print(f" No products on page {page_num} — stopping.")
            break

        all_products.extend(products)
        print(f" Parsed {len(products)} products (total: {len(all_products)})")

    return all_products


def run(run_date=None, run_hour=None):
    print("=" * 50)
    print("Smartprix Scraper Starting")
    print(f"Run time: {datetime.now(timezone.utc).replace(tzinfo=None)}")
    print("=" * 50)

#partition_dt drives the s3 folder path. If Airflow supplied run_date/run_hour, use that(so bronze matches what Glue will look for). Otherwise, fallback to actual current time - keeps standalone/local runs working unchanged.

    if run_date and run_hour:
        partition_dt = datetime.strptime(run_date, '%Y-%m-%d').replace(hour=int(run_hour))
        print(f"Using provided run_date/run_hour for partitioning: {run_date} {run_hour}")
    else:
        partition_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        print(f"No run_date/run_hour provided. Using current UTC time for partitioning: {partition_dt}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
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

        for category_name, base_url in CATEGORIES.items():
            print(f"\nCategory: {category_name.upper()}")
            products = scrape_category(page, category_name, base_url)

            if products:
                s3_key = upload_to_s3(products, category_name, partition_dt)
                print(f"\nDone: {len(products)} products → {s3_key}")
            else:
                print(f"Nothing scraped for {category_name}")

        browser.close()

    print("\nScraper finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smartprix Scraper")
    parser.add_argument("--run_date", type=str, default=None)
    parser.add_argument("--run_hour", type=str, default=None)
    args = parser.parse_args()
    run(run_date=args.run_date, run_hour=args.run_hour)
