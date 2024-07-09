{{
    config(
        materialized='table'
    )
}}

with cycling_data as (
    select * from {{ ref('fact_cycling') }}
)
select 
    Borough,
    Road_type,
    sum(Count) as Total_Count
from cycling_data
group by Borough, Road_type
