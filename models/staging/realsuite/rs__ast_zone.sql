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
        {{ rs_audit_cols(source('realsuite', 'ast_zone')) }},
        {{ cdc_audit_cols(source('realsuite', 'ast_zone')) }}
    from source
)

select * from renamed
