with dim_uom as (
    select distinct 
         {{ surrogate_key('uom') }} as uom_key,
         cast(uom as varchar(500)) as uom,
         cast('N' as char(1)) as softdelete_flag
    from {{ ref('rs__ast_uniformat') }} 
    where 
        softdelete_flag='N'
)

select * from dim_uom
