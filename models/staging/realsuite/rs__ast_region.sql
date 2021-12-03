with source as (
    select * from {{ source('realsuite', 'ast_region') }}
),

renamed as (
    select
        asset_region_id,
        client_id,
        region_code,
        region_name,
        cast(round(price_offset,4) as number(12,5)) as asset_region_price_offset,
        status,
        {{ rs_audit_col() }},
        {{ cdc_timestamp_col() }}
    from source
    where
    {{ cdc_softdelete_filter() }} 
)

select * from renamed