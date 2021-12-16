with dim_materialtype as (
    select distinct 
        {{ surrogate_key('material_type') }} as materialtype_key,
        cast(material_type as varchar(1000)) as material_type,
        cast('N' as char(1)) as softdelete_flag
    from 
        {{ ref('rs__ast_uniformat') }} 
    where
        softdelete_flag='N'
)

select * from dim_materialtype 