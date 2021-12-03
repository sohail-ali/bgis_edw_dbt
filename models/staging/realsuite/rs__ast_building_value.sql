with source as (
    select * from {{ source('realsuite', 'ast_building_value') }}
),

renamed as (
    select
        building_pid,
        client_id,
        building_id,
        asset_region_id,
        cast(interior_value as number(12,2)) as interior_replacement_value_dollars_per_sq,
        cast(shell_value as number(12,2)) as shell_replacement_value_dollars_per_sqft,
        cast(ffe_value as number(12,2)) as ff_and_e_replacement_value_dollars_per_sqft,
        {{ rs_audit_col() }},
        {{ cdc_timestamp_col() }}
    from source
    where
        {{ cdc_softdelete_filter() }} 
)

select * from renamed