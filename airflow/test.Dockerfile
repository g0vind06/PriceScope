FROM python:3.11-slim

RUN pip install --no-cache-dir playwright beautifulsoup4 lxml boto3 python-dotenv
RUN playwright install --with-deps chromium

# COPY test_playwright.py .
# CMD ["python", "test_playwright.py"]

WORKDIR /pricescope

COPY scrapers/ scrapers/
COPY config/ config/
COPY .env .env

CMD ["python", "scrapers/smartprix/smartprix_scraper.py"]



