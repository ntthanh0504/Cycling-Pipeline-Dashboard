{{
    config(
        materialized='table'
    )
}}

with data_2018 as (
    select *
    from {{ ref('stg_data-2018') }}
),
data_2019 as (
    select *
    from {{ ref('stg_data-2019') }}
),
data_2020 as (
    select *
    from {{ ref('stg_data-2020') }}
),
data_2021 as (
    select *
    from {{ ref('stg_data-2021') }}
),
data_2022 as (
    select *
    from {{ ref('stg_data-2022') }}
),
cycling_unioned as (
    select * from data_2018
    union all
    select * from data_2019
    union all
    select * from data_2020
    union all
    select * from data_2021
    union all
    select * from data_2022
),
monitoring_locations as (
    select * from {{ ref('mo_locations') }}
)

select cyu.*, mol.*
from cycling_unioned as cyu 
inner join monitoring_locations as mol 
on cyu.UnqID = mol.Site_ID