
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select category_key
from "dev"."marts"."dim_category"
where category_key is null



  
  
      
    ) dbt_internal_test