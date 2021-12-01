with dim_assetproject as (
    select 
        cast(hash(p.asset_project_id) as number(38)) as assetproject_key,
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
    left join {{ ref('stg_dim_assetproject_comment') }} c on 
        p.asset_project_id = c.asset_project_id 
)

select * from dim_assetproject 