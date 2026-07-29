{{ config(materialized='table', dist='all') }}

WITH distinct_platforms AS (
    SELECT DISTINCT source AS platform_name
    FROM {{ ref("stg_smartprix_prices") }}
),

enriched AS(
    SELECT 
    row_number() OVER (ORDER BY platform_name) AS platform_key,
    platform_name,
    platform_name AS platform_url  --to be refined laters
    FROM distinct_platforms
)

SELECT * FROM enriched ORDER BY platform_key