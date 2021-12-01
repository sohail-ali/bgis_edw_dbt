with dim_uniformat_unknown as 
(
select 
    cast(0  as number) as uniformat_key,
    cast('N/A' as varchar(500)) as uniformat_level1,
    cast('N/A' as varchar(500)) as uniformat_level2,
    cast('N/A' as varchar(500)) as uniformat_level3,
    cast('N/A' as varchar(500)) as uniformat_level4,
    cast('N/A' as varchar(500)) as uniformat_level1_name,
    cast('N/A' as varchar(500)) as uniformat_level2_name,
    cast('N/A' as varchar(500)) as uniformat_level3_name,
    cast('N/A' as varchar(500)) as uniformat_level4_name
from DUAL 
)

select * from dim_uniformat_unknown