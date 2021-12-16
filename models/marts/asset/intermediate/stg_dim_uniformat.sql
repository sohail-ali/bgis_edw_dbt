with 
uniformat_level1 as (
    select 
        cast(upper(trim(lookup_value)) as varchar(500)) as uniformat_level1,
        cast(upper(trim(lookup_definition)) as varchar(500)) as uniformat_level1_name
    from 
        {{ ref('rs__lookup_code') }} 
    where
        lookup_type_class ='AST.UNIFORMAT_LEVEL1'
)
, uniformat_level2 as (
    select 
        cast(upper(trim(lookup_value)) as varchar(500)) as uniformat_level2,
        cast(upper(trim(lookup_definition)) as varchar(500)) as uniformat_level2_name
    from 
        {{ ref('rs__lookup_code') }} 
    where
        lookup_type_class ='AST.UNIFORMAT_LEVEL2'
)

, uniformat_level3 as (
    select 
        cast(upper(trim(lookup_value)) as varchar(500)) as uniformat_level3,
        cast(upper(trim(lookup_definition)) as varchar(500)) as uniformat_level3_name
    from 
        {{ ref('rs__lookup_code') }} 
    where
        lookup_type_class ='AST.UNIFORMAT_LEVEL3'
)

, uniformat_level4 as (
    select 
        cast(upper(trim(lookup_value)) as varchar(500)) as uniformat_level4,
        cast(upper(trim(lookup_definition)) as varchar(500)) as uniformat_level4_name
    from 
        {{ ref('rs__lookup_code') }} 
    where
        lookup_type_class ='AST.UNIFORMAT_LEVEL4'
)

, ast_uniformat as (
    select distinct 
        uniformat_level1,
        uniformat_level2,
        uniformat_level3,
        uniformat_level4
    from 
        {{ ref('rs__ast_uniformat') }} 
    where
        softdelete_flag='N'
)

, dim_uniformat as (
    select 
        {{ surrogate_key('u.uniformat_level1','u.uniformat_level2','u.uniformat_level3','u.uniformat_level4') }} as uniformat_key,
        u.uniformat_level1,
        u.uniformat_level2,
        u.uniformat_level3,
        u.uniformat_level4,
        l1.uniformat_level1_name,
        l2.uniformat_level2_name,
        l3.uniformat_level3_name,
        l4.uniformat_level4_name
    from 
    ast_uniformat u
    left join uniformat_level1 l1 on 
        u.uniformat_level1 = l1.uniformat_level1
    left join uniformat_level2 l2 on 
        u.uniformat_level2 = l2.uniformat_level2
    left join uniformat_level3 l3 on 
        u.uniformat_level3 = l3.uniformat_level3
    left join uniformat_level4 l4 on 
        u.uniformat_level4 = l4.uniformat_level4
)

select * from dim_uniformat
