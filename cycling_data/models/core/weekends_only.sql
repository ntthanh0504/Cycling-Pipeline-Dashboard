{{
    config(
        materialized='table'
    )
}}

with cycling_data as (
    select * from {{ ref('fact_cycling') }}
)
select 
    Day,
    sum(Count) as Total_Count
from cycling_data
group by Day
