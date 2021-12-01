with dim_materialtype as (
    select distinct 
        cast(hash(material_type) as number(38)) as materialtype_key,
        cast(material_type as varchar(1000)) as material_type,
        cast('N' as char(1)) as softdelete_flag
    from 
        {{ ref('rs__ast_uniformat') }} 
    where
        softdelete_flag='N'
)

select * from dim_materialtype 