with dim_client_unknown as 
(
select 
      cast(0 as number(38)) as client_key
    , cast(-1 as number(38)) as client_id
    , cast('N/A' as varchar(500)) as client_name
    , cast('N/A' as varchar(1000)) as client_desc
    , cast('N/A' as varchar(500))  as client_class_cd
    , cast(TRUE as boolean) as activeInd
    , cast(-1 as number(38)) as pa_organization_id
    , cast('N/A' as varchar(500)) as woprefix
    , cast('N' as char(1)) as softdelete_flag
    , cast(NULL as number(38)) as weather_normalization
    , cast('N/A' as varchar(500)) as uom_landarea
    , cast('N/A' as varchar(500)) as uom_buildingarea
    , cast('N/A' as varchar(500)) as uom_landelevation
    , cast('N/A' as varchar(250)) as industry_name
    , cast('N/A' as varchar(250)) as industry_group_code
    , cast(FALSE as boolean) as benchmark_flag
    , cast(TRUE as boolean) as lease_exclusion_flag
    , cast(NULL as char(1)) as pds_client_flag
from DUAL 
)

select * from dim_client_unknown