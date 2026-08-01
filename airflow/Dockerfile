FROM apache/airflow:2.10.3

USER root

ENV PIP_USER=false
# Airflow's base image sets PIP_USER=true ehcih forces pip to install into the airflow user's personal site-packages. That conflicts with running as root (needed below for apt-get/chromium system deps) so we override it to install system-wide instead.

# System-level dependencies for Chromium, installed manually here so that
# pip (run later as the airflow user) never needs root — Airflow's base
# image hard-blocks any pip invocation running as root, independent of
# PIP_USER or any other env var.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libcairo2 libxext6 \
    fonts-liberation libu2f-udev libvulkan1 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

#Pre-create /opt/dbt_venv and hand ownership to the airflow user, since /opt is root-owned by default and the venv itself must be created below as a non-root user
RUN mkdir -p /opt/dbt_venv && chown -R airflow: /opt/dbt_venv

USER airflow

# Airflow-facing packages: scraper dependencies + AWS provider.
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    playwright \
    beautifulsoup4 \
    lxml \
    python-dotenv \
    boto3

# Chromium binary only — system deps already installed above as root,
# so --with-deps is neither needed nor usable here (no root access).
RUN python -m playwright install chromium

# dbt lives in an ISOLATED virtualenv, separate from Airflow's own
# environment — installing dbt-redshift alongside Airflow's pinned
# dependencies causes pip's resolver to blow up (ResolutionTooDeep).
RUN python -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir dbt-redshift