with dim_materialtype_unknown as 
(
select 
      cast(0  as number) as materialtype_key
    , cast('N/A' as varchar(500)) as material_type
    , cast('N' as char(1)) AS softdelete_flag
from DUAL 
)

select * from dim_materialtype_unknown