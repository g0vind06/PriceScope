
  
    

  create  table
    "dev"."marts_staging"."stg_smartprix_prices__dbt_tmp"
    
    
    
  as (
       --since views can't reference external tables, we override the materialized for staging as table.

with source as (
    select
        *
    from "dev"."spectrum_silver"."stg_price_snapshots"
),

renamed as (
    select
        product_id,
        product_name,
        product_url,
        brand,
        category,
        current_price,
        mrp,
        discount_pct,
        has_discount,
        rating,
        is_rating_available,
        source,
        scraped_at,
        run_date,
        run_hour
    from source 
)

select * from renamed
  );
  