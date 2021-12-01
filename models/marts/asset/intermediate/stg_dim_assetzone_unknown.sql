with dim_assetzone_unknown as 
(
select 
      cast(0  as number) as assetzone_key
    , cast(-1 as number) as assetzone_id
    , cast(-1 as number) as client_id
    , cast('N/A' as varchar(500)) as assetzone_code
    , cast('N/A' as varchar(500)) as assetzone_name
    , cast('N/A' as varchar(500)) AS assetzone_status
    , cast('N' as char(1)) AS softdelete_flag
from DUAL 
)

select * from dim_assetzone_unknown