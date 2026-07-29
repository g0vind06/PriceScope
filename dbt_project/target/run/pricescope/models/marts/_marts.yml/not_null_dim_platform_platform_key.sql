
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select platform_key
from "dev"."marts"."dim_platform"
where platform_key is null



  
  
      
    ) dbt_internal_test