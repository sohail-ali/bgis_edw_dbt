with source as (
    select * from {{ source('realsuite', 'ast_zone') }}
),

renamed as (
    
    select
        asset_zone_id as assetzone_id,
        zone_code as assetzone_code,
        client_id,
        building_id,
        zone_name as assetzone_name,
        status as assetzone_status,
        createdon as created_date,
        createdby,
        updatedon as updated_date,
        updatedby,
        {{ cdc_timestamp_col() }},
        {{ cdc_softdelete_col() }} 
    from source
)

select * from renamed
