
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_key
from "dev"."marts"."fact_price_snapshots"
where product_key is null



  
  
      
    ) dbt_internal_test