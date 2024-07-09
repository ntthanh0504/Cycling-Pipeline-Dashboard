{{ 
    config(materialized='table') 
}}

select
    Site_ID,Location_description,Borough,Functional_area_for_monitoring,Road_type,Old_site_ID_legacy,Easting_UK_Grid,Northing_UK_Grid,Latitude,Longitude
from {{ref('monitoring_locations')}}