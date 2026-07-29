
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    platform_key as unique_field,
    count(*) as n_records

from "dev"."marts"."dim_platform"
where platform_key is not null
group by platform_key
having count(*) > 1



  
  
      
    ) dbt_internal_test