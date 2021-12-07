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
        {{ cdc_audit_cols(source('realsuite', 'rpro_moduleoption')) }}
    from source
    where 
      softdelete_flag ='N'
)

select * from renamed
