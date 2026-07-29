
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select snapshot_id
from "dev"."marts"."fact_price_snapshots"
where snapshot_id is null



  
  
      
    ) dbt_internal_test