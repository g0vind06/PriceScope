{{ config(materialized='table', dist='all',sort='date_key') }}  --dist = all -> copies the entire table(dim_date) to every compute nodes from the leader node...since this is used constantly(to join with fact tables) and is small(730 rows), every compute node having a local copy helps - avoid network shuffling during joins.

with digits as(
    select 0 as digit 
    union all select 1
    union all select 2
    union all select 3
    union all select 4
    union all select 5
    union all select 6
    union all select 7
    union all select 8
    union all select 9
),

numbers as(
    select d1.digit + d2.digit*10 + d3.digit*100 as n
    from digits d1
    cross join digits d2
    cross join digits d3  -- generates 0-999
),

date_spine as(
    select (date '2026-01-01' + n)::date as date_day
    from numbers where n<=729                                                                --generate_series(0, 729) as n --covers 2026-01-01 through 2027-12-31
),  --generate_series() is a leader node only function, it can run in plain select but not to CREATE table statement.

enriched as (
    select 
    to_char(date_day, 'YYYYMMDD')::int as date_key,
    date_day as full_date,
    extract(day from date_day) as day_of_month,
    to_char(date_day, 'Day') as day_name,
    extract(dow from date_day) as day_of_week, --0=Sunday , 6=Saturday
    extract(week from date_day) as week_number,
    extract(month from date_day) as month,
    to_char(date_day, 'Month') as month_name,
    extract(quarter from date_day) as quarter,
    extract(year from date_day) as year,
    case 
        when extract(dow from date_day) in (0, 6) then true
        else false
    end as is_weekend
    from date_spine
)

select * from enriched order by date_key
