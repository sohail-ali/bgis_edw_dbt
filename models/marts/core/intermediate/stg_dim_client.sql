with dim_client as (

    select client.client_id 
     , client.client_name 
     , client.description as client_desc 
     , client.class_code as client_class_cd 
     , case when client.status_code='A' then TRUE else FALSE end as activeInd 
     , ifnull(client.pa_organization_id,-1) as pa_organization_id 
     , client.woprefix 
     , client.cdc_softdelete_flag as softdelete_flag
     , rfin.weather_normalization 
     , rpro.land_area_uom as uom_landarea 
     , rpro.building_area_uom as uom_buildngarea 
     , rpro.land_elevation_uom as uom_landelevation
     , benchmark.industry_name 
     , benchmark.industry_group_code 
     , case when benchmark.benchmark_flag is null then FALSE else TRUE end as benchmark_flag       
     , case when lease.client_id is null then FALSE else TRUE end as lease_client_flag 
     , case when pds.client_id is null then FALSE else TRUE end as pds_client_flag 
    from {{ ref('stg_client')}} client 
    left join  {{ ref('stg_rfin_moduleoption') }} rfin 
      on client.client_id = rfin.client_id
    left join  {{ ref('stg_rpro_moduleoption') }} rpro 
        on client.client_id = rpro.client_id
    left join  {{ ref('lease_client') }} lease 
        on client.client_id = lease.client_id
    left join  {{ ref('pds_client') }} pds 
        on client.client_id = pds.client_id
    left join  {{ ref('client_industry') }} benchmark 
        on client.client_id = benchmark.client_id        
)

select * from dim_client order by 1 