
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    category_key as unique_field,
    count(*) as n_records

from "dev"."marts"."dim_category"
where category_key is not null
group by category_key
having count(*) > 1



  
  
      
    ) dbt_internal_test