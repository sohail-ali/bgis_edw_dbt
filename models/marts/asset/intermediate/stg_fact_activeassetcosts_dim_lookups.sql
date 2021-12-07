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

, fact_activeassetcosts_lookup as (
    select 
        coalesce(ap.assetproject_key,0) as assetproject_key,
        coalesce(c.client_key,0) as client_key,
        f.building_key as building_key,
        coalesce(az.assetzone_key,0) as assetzone_key,
        coalesce(s.assetstatus_key,0) as assetstatus_key,
        coalesce(m.materialtype_key,0) as materialtype_key,
        coalesce(uf.uniformat_key,0) as uniformat_key,
        coalesce(u.uom_key,0) as uom_key,
        practicallife_replacement_date,
        coalesce(pr_cp.clientperiod_key,0) as practicallife_replacement_clientperiod_key,
        adjusted_practicallife_replacement_date,
        coalesce(apr_cp.clientperiod_key,0) as adjusted_practicallife_replacement_clientperiod_key,
        meanservicelife_replacement_date,
        coalesce(mr_cp.clientperiod_key,0) as meanservicelife_replacement_clientperiod_key,
        building_item_number,
        step,
        replacement_cost,
        adjusted_replacement_cost,
        field_replacement_estimate_cost
    from
        fact_activeassetcosts f 
    left join {{ ref('dim_client') }} c 
        on f.client_key = c.client_key 
    left join {{ ref('dim_assetproject') }} ap 
        on f.assetproject_key = ap.assetproject_key 
    left join {{ ref('dim_assetzone') }} az
        on f.assetzone_key = az.assetzone_key 
    left join {{ ref('dim_assetstatus') }} s
        on f.assetstatus_key = s.assetstatus_key 
    left join {{ ref('dim_materialtype') }} m
        on f.materialtype_key = m.materialtype_key    
    left join {{ ref('dim_uniformat') }} uf
        on f.uniformat_key = uf.uniformat_key 
    left join {{ ref('dim_uom') }} u
        on f.uom_key = u.uom_key
    left join {{ ref('dim_clientperiod') }} pr_cp 
        on f.practicallife_replacement_clientperiod_key = pr_cp.clientperiod_key 
    left join {{ ref('dim_clientperiod') }} apr_cp 
        on f.adjusted_practicallife_replacement_clientperiod_key = apr_cp.clientperiod_key 
    left join {{ ref('dim_clientperiod') }} mr_cp 
        on f.meanservicelife_replacement_clientperiod_key = mr_cp.clientperiod_key 
)

select * from fact_activeassetcosts_lookup
