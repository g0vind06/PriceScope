

WITH base AS (
    SELECT 
        product_id,
        category,
        source,
        current_price,
        mrp,
        discount_pct,
        has_discount,
        rating,
        is_rating_available,
        scraped_at,
        run_date,
        run_hour
    FROM "dev"."marts_staging"."stg_smartprix_prices"
),

joined AS (
    SELECT
        b.product_id AS product_key,
        cat.category_key,
        plat.platform_key,
        dd.date_key,
        b.current_price,
        b.mrp,
        b.discount_pct,
        b.has_discount,
        b.rating,
        b.is_rating_available,
        b.scraped_at,
        b.run_date,
        b.run_hour
    FROM base b
    LEFT JOIN "dev"."marts"."dim_category" cat ON b.category=cat.category_name
    LEFT JOIN "dev"."marts"."dim_platform" plat ON b.source=plat.platform_name
    LEFT JOIN "dev"."marts"."dim_date" dd ON b.run_date=dd.full_date
),

with_trend AS (
    SELECT
        *,
        lag(current_price) OVER (PARTITION BY product_key ORDER BY scraped_at) AS previous_price
    FROM joined
),

final AS (
    SELECT
        row_number() OVER (ORDER BY run_date, product_key, scraped_at) as snapshot_id,
        product_key,
        category_key,
        platform_key,
        date_key,
        current_price,
        mrp,
        discount_pct,
        has_discount,
        rating,
        is_rating_available,
        previous_price,
        current_price-previous_price AS price_change,
        scraped_at,
        run_date,
        run_hour
    FROM with_trend
)

SELECT * FROM final ORDER BY run_date, product_key, scraped_at