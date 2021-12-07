with fact_activeassetcosts_forecast as (
    select
        asset_id, 
        building_item_number, 
        asset_project_id, 
        client_id, 
        building_id, 
        uniformat_level1, 
        uniformat_level2, 
        uniformat_level3, 
        uniformat_level4, 
        status AS status, 
        material_type, 
        uom, 
        assetzone_id, 
        replacement_cost, 
        adjusted_replacement_cost, 
        field_replacement_estimate_cost, 
        case when practicallife_replacement_date > forecast_end_date then NULL 
            else practicallife_replacement_date end as drv_practicallife_replacement_date, 
        {{ full_month_name('drv_practicallife_replacement_date') }} as practicallife_replacement_full_month,
        case when adjusted_practicallife_replacement_date > forecast_end_date then NULL 
            else  adjusted_practicallife_replacement_date end as drv_adjusted_practicallife_replacement_date,        
        {{ full_month_name('drv_adjusted_practicallife_replacement_date') }} as adjusted_practicallife_replacement_full_month,
        case when meanservicelife_replacement_date > forecast_end_date then NULL 
            else meanservicelife_replacement_date end as drv_meanservicelife_replacement_date,
        {{ full_month_name('drv_meanservicelife_replacement_date') }} as meanservicelife_replacement_full_month,
        step,
        forecast_end_date        
    from  {{ ref('stg_fact_activeassetcosts') }}
    where 
        (
            not (
            practicallife_replacement_date > forecast_end_date and 
            adjusted_practicallife_replacement_date > forecast_end_date and 
            meanservicelife_replacement_date > forecast_end_date) 
        or step=1
        )
)

select 
    asset_id, 
    building_item_number, 
    asset_project_id, 
    client_id, 
    building_id, 
    uniformat_level1, 
    uniformat_level2, 
    uniformat_level3, 
    uniformat_level4, 
    status AS status, 
    material_type, 
    uom, 
    assetzone_id, 
    cast(replacement_cost as number(18,2)) as replacement_cost, 
    cast(adjusted_replacement_cost as number(18,2)) as adjusted_replacement_cost, 
    cast(field_replacement_estimate_cost as number(18,2)) as field_replacement_estimate_cost, 
    drv_practicallife_replacement_date as practicallife_replacement_date, 
    practicallife_replacement_full_month,
    drv_adjusted_practicallife_replacement_date as adjusted_practicallife_replacement_date,        
    adjusted_practicallife_replacement_full_month,
    drv_meanservicelife_replacement_date as meanservicelife_replacement_date,
    meanservicelife_replacement_full_month,
    cast(step as number(38)) as step,
    forecast_end_date        
from fact_activeassetcosts_forecast