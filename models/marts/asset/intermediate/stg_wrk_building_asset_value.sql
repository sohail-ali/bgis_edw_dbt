with building_asset_value as (
    select 
        v.building_id,
        r.asset_region_id,
        r.region_code,
        r.region_name,
        r.status,
        r.asset_region_price_offset,
        v.interior_replacement_value_dollars_per_sq,
        v.shell_replacement_value_dollars_per_sqft,
        v.ff_and_e_replacement_value_dollars_per_sqft
    from 
    {{ ref('rs__ast_region') }} r 
    join {{ ref('rs__ast_building_value') }} v 
        on r.asset_region_id=v.asset_region_id
)

select * from building_asset_value