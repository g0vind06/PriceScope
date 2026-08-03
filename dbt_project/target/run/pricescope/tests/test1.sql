
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  --Fails for scripts that returns rows.
--Passes for scripts that returns no rows.

SELECT * FROM "dev"."marts_staging"."stg_smartprix_prices" WHERE mrp < 0  --negative mrp is invalid, so this should return no rows. If it returns rows, then the test fails.
  
  
      
    ) dbt_internal_test