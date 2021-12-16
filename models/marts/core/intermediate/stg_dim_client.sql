with dim_client as (
    select 
     {{ surrogate_key('client.client_id') }} as client_key,
     client.client_id,
     client.client_name,
     client.description as client_desc,
     client.class_code as client_class_cd,
     case when client.status_code='A' then TRUE else FALSE end as activeInd,
     ifnull(client.pa_organization_id,-1) as pa_organization_id,
     client.woprefix,
     client.softdelete_flag,
     rfin.weather_normalization,
     rpro.land_area_uom as uom_landarea,
     rpro.building_area_uom as uom_buildingarea,
     rpro.land_elevation_uom as uom_landelevation,
     benchmark.industry_name,
     benchmark.industry_group_code,
     case when benchmark.benchmark_flag is null then FALSE else TRUE end as benchmark_flag,
     case when lease.client_id is null then FALSE else TRUE end  as lease_exclusion_flag,
     cast(case when pds.client_id is null then NULL else 'Y' end as char(1)) as pds_client_flag
    from {{ ref('rs__client')}} client 
    left join  {{ ref('rs__rfin_moduleoption') }} rfin 
      on client.client_id = rfin.client_id
    left join  {{ ref('rs__rpro_moduleoption') }} rpro 
        on client.client_id = rpro.client_id
    left join  {{ ref('lease_exclude_client') }} lease 
        on client.client_id = lease.client_id
    left join  {{ ref('pds_include_client') }} pds 
        on client.client_id = pds.client_id
    left join  {{ ref('mdm__client_industry') }} benchmark 
        on client.client_id = benchmark.client_id
)
select * from dim_client 
