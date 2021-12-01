with dim_assetstatus_unknown as 
(
select 
      cast(0 as number(38)) as assetstatus_key
    , cast('N/A' as varchar(500)) as status
from DUAL 
)

select * from dim_assetstatus_unknown