
    
    

select
    category_key as unique_field,
    count(*) as n_records

from "dev"."marts"."dim_category"
where category_key is not null
group by category_key
having count(*) > 1


