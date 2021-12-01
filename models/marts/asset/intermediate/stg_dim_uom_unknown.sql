with dim_uom_unknown as 
(
select 
      cast(0 as number(38)) as uom_key,
      cast('N/A' as varchar(500)) as uom,
      cast('N' as char(1)) as softdelete_flag

from DUAL 
)

select * from dim_uom_unknown