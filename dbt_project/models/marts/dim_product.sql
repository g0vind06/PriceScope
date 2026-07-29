{{config(materialized='table', dist='all') }}

--product_details : one row per product

WITH ranked AS (
    SELECT 
        product_id,
        product_name,
        brand,
        category,
        product_url,
        scraped_at,
        row_number() OVER (PARTITION BY product_id ORDER BY scraped_at DESC) AS rn   --if any product details are modified, we will pull the latest one using scraped_at.
    FROM {{ ref("stg_smartprix_prices") }}
),

latest_per_product AS (
    SELECT
        product_id AS product_key,
        product_name,
        brand,
        category,
        product_url
    FROM ranked WHERE rn=1
)

SELECT * FROM latest_per_product ORDER BY product_key