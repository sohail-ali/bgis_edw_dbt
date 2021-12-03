with dim_assetzone as (
    select 
        {{ surrogate_key('assetzone_id') }} as assetzone_key,
        assetzone_id,
        client_id,
        assetzone_code,
        assetzone_name,
        assetzone_status,
        softdelete_flag
    from {{ ref('rs__ast_zone')}} 
)
select * from dim_assetzone 
