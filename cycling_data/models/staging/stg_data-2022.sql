{{
    config(
        materialized='view'
    )
}}

with cyclingdata as 
(
  select *,
    row_number() over(partition by UnqID, Date, Time) as rn
  from {{ source('staging','data-2022') }}
  where UnqID is not null 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['UnqID', 'Date', 'Time']) }} as cyclingid,
    cast(Year as string) as Year,
    cast(UnqID as string) as UnqID,
    
    -- timestamps
    -- cast(Date as date) as Date,
    PARSE_DATE('%d/%m/%Y', Date) as Date,
    -- cast(Time as timestamp) as Time,
    PARSE_TIMESTAMP('%H:%M:%S', Time) as Time,
    
    -- trip info
    cast(Weather as string) as Weather,
    cast(Day as string) as Day,
    cast(Round as string) as Round,
    cast(Dir as string) as Dir,
    cast(Path as string) as Path,
    cast(Mode as string) as Mode,
    cast(Count as int) as Count

from cyclingdata
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
