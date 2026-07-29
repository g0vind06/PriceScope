

WITH distinct_categories AS (
    SELECT DISTINCT category AS category_name
    FROM "dev"."marts_staging"."stg_smartprix_prices"
),

enriched AS (
    SELECT row_number() OVER (ORDER BY category_name) AS category_key, 
    category_name
    FROM distinct_categories
)

SELECT * FROM enriched ORDER BY category_key