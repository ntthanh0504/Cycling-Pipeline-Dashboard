{{
    config(
        materialized='table'
    )
}}

with cycling_data as (
    select 
        *,
        split(Year, ' ')[0] as Year_Only,
        split(split(Year, ' ')[1], '(')[0] as Quarter
    from {{ ref('fact_cycling') }}
)
select 
    Year_Only as Year,
    Quarter,
    sum(Count) as Total_Count
from cycling_data
group by Year, Quarter