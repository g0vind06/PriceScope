
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select current_price
from "dev"."marts_staging"."stg_smartprix_prices"
where current_price is null



  
  
      
    ) dbt_internal_test