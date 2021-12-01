with source as (
    
    select * from {{ source('realsuite', 'rpro_moduleoption') }}
    
),

renamed as (
    
    select
        moduleoption_id,
        client_id,
        building_area_uom,
        land_area_uom,
        land_elevation_uom,
        {{ cdc_timestamp_col() }}
    from source
    where 
      {{ cdc_softdelete_filter() }}
)

select * from renamed
