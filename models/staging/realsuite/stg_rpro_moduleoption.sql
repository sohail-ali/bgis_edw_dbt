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
        cdc_modified_datetime,
        decode(ifnull(cdc_softdelete_flag,''),'D','Y','N') as cdc_softdelete_flag
    from source

)

select * from renamed
