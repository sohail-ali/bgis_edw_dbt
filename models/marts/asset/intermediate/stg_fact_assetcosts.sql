with 
ast_asset as (
    select asset_id,
        asset_project_id,
        client_id,
        uniformat_id,
        quantity as material_quantity,
        decode(replacement_responsibility,'Y',TRUE,FALSE) as client_replacement_responsibility_ind,
        practical_life_adjustment,
        field_replacement_estimate,
        field_estimate_date
    from {{ ref('rs__ast_asset') }} a
    where 
        softdelete_flag='N'
)

, equipment as (
        select 
            equipment_id,
            client_id,
            building_id,
            eqnum as building_item_number,
            client_eqnum as client_building_item_number,
            status,
            building_item_zone_id as asset_zone_id,
            description,
            install_date as installation_date,
            actual_decomissioned_date as decomissioned_date,
            criticality_code
    from {{ ref('rs__equipment') }}  
    where 
        softdelete_flag='N'
        and building_id is not null
        and install_date is not null
)

, ast_uniformat as (
        select 
            uniformat_id, 
            uniformat_level1, 
            uniformat_level2, 
            uniformat_level3, 
            uniformat_level4, 
            material_type, 
            mean_service_life, 
            practical_life, 
            unit_price,
            uom
        from {{ ref('rs__ast_uniformat') }} 
        where 
            softdelete_flag ='N' 
)

, asset_comment as (
    select 
        object_id as asset_id,
        comment
    from {{ ref('stg_wrk_ast_comment_pivot')}} 
    where 
        object_class = 'AST_ASSET'
)

, building_asset_value as (
    select
        building_id,
        ifnull(asset_region_price_offset,1) as asset_region_price_offset
    from {{ ref('stg_wrk_building_asset_value')}} 
)

, equipment_criticality as (
    select
        client_id,
        lookup_value as criticality_code,
        lookup_definition as building_item_criticality
    from {{ ref('rs__lookup_code') }} 
    where 
        lookup_type_class ='RCND.EQUIPMENT_CRITICALITY'       
)

, fact_assetcosts as (
    select
        a.asset_id,
        a.asset_project_id,
        a.client_id,
        e.building_id,
        a.uniformat_id,
        uniformat_level1,
        uniformat_level2,
        uniformat_level3,
        uniformat_level4,
        e.building_item_number,
        e.client_building_item_number,
        a.material_quantity,
        a.client_replacement_responsibility_ind,
        u.practical_life + a.practical_life_adjustment as practical_life_adjustment,
        ifnull(a.field_replacement_estimate,0) as field_replacement_estimate,
        a.field_estimate_date,
        e.status,
        e.asset_zone_id,
        u.material_type,
        u.mean_service_life,
        u.practical_life,
        u.unit_price,
        u.uom,
        ac.comment,
        e.description,
        asset_region_price_offset,
        u.unit_price * material_quantity * av.asset_region_price_offset as replacement_cost,
        ifnull(field_replacement_estimate,replacement_cost) as adjusted_replacement_cost,
        installation_date,
        upper(monthname(installation_date)) ||'-'|| year(installation_date) as installation_full_month,
        dateadd(year,practical_life ,installation_date) as practicallife_replacement_date,
        upper(monthname(practicallife_replacement_date)) ||'-'|| year(practicallife_replacement_date) as practicallife_replacement_full_month,
        dateadd(year,practical_life + practical_life_adjustment,installation_date) as adjusted_practicallife_replacement_date,
        upper(monthname(adjusted_practicallife_replacement_date))||'-'||year(adjusted_practicallife_replacement_date) as adjusted_practicallife_replacement_full_month,
        dateadd(year,mean_service_life,installation_date) as meanservicelife_replacement_date,
        upper(monthname(meanservicelife_replacement_date))||'-'||year(meanservicelife_replacement_date) as meanservicelife_replacement_full_month,
        decomissioned_date,
        upper(monthname(decomissioned_date))||'-'||year(decomissioned_date) as decomissioned_full_month,
        ec.building_item_criticality
    from 
    ast_asset a
    join equipment e 
        on a.asset_id = e.equipment_id
    join ast_uniformat u 
        on a.uniformat_id = u.uniformat_id
    left join building_asset_value av 
        on e.building_id = av.building_id
    left join asset_comment ac
        on a.asset_id = ac.asset_id 
    left join equipment_criticality ec 
        on  e.client_id = ec.client_id 
        and e.criticality_code = ec.criticality_code
)

select * from fact_assetcosts