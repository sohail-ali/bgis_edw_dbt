with fact_activeassetcosts as (
    select
        {{ surrogate_key('asset_project_id') }} as assetproject_key,
        {{ surrogate_key('client_id') }} as client_key,
        {{ surrogate_key('building_id') }} as building_key,
        {{ surrogate_key('assetzone_id') }} as assetzone_key,
        {{ surrogate_key('status') }} as assetstatus_key,
        {{ surrogate_key('material_type') }} as materialtype_key,
        {{ surrogate_key('uniformat_level1','uniformat_level2','uniformat_level3','uniformat_level4') }} as uniformat_key,
        {{ surrogate_key('uom') }} as uom_key,
        practicallife_replacement_date,
        {{ surrogate_key('client_id','practicallife_replacement_full_month') }} as practicallife_replacement_clientperiod_key,
        adjusted_practicallife_replacement_date,
        {{ surrogate_key('client_id','adjusted_practicallife_replacement_full_month') }} as adjusted_practicallife_replacement_clientperiod_key,
        meanservicelife_replacement_date,
        {{ surrogate_key('client_id','meanservicelife_replacement_full_month') }} as meanservicelife_replacement_clientperiod_key,
        building_item_number,
        step,
        replacement_cost,
        adjusted_replacement_cost,
        field_replacement_estimate_cost
    from {{ ref('stg_fact_activeassetcosts_forecast') }}
)

select * from fact_activeassetcosts
