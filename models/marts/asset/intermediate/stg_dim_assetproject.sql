with ast_project_comment as (
    select 
        object_id as asset_project_id,
        comment
    from {{ ref('stg_wrk_ast_comment_pivot')}} 
    where 
        object_class = 'AST_PROJECT'
)

, dim_assetproject as (
    select 
        {{ surrogate_key('p.asset_project_id') }} as assetproject_key,
        p.asset_project_id,
        p.assetproject_name as name,
        p.assetproject_status,
        p.assetproject_type,
        p.assetproject_priority,
        p.planned_replacement_date,
        p.assetproject_number,
        p.assetproject_category,
        c.comment as comments,
        p.softdelete_flag
    from 
        {{ ref('rs__ast_project') }} p 
    left join ast_project_comment c on 
        p.asset_project_id = c.asset_project_id 
)

select * from dim_assetproject 