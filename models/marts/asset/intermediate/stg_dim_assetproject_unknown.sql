with dim_assetproject_unknown as 
(
select 
    cast(0 as number) AS assetproject_key,
    cast(-1 as number) AS asset_project_id,
    cast('N/A' as varchar(100)) AS name,
    cast('N/A' as varchar(50)) AS assetproject_status,
    cast('N/A' as varchar(50)) AS assetproject_type,
    cast('N/A' as varchar(50)) AS assetproject_priority,
    date('1900-01-01') AS planned_replacement_date,
    cast('N/A' as varchar(15)) AS assetproject_number,
    cast('N/A' as varchar(100)) AS assetproject_category,
    cast('N/A' as varchar(6000)) AS comments,
    cast('N' as char(1)) AS softdelete_flag
from DUAL 
)

select * from dim_assetproject_unknown