with assetproject_costs as (
    select
        asset_project_id,
        sum(replacement_cost) as replacement_cost,
        sum(adjusted_replacement_cost) as adjusted_replacement_cost
    from 
        {{ ref('stg_fact_assetcosts') }}
    where
        asset_project_id is not null
    group by 1
)

, fact_assetprojects as (
    select 
        asset_project_id,
        'Capital' as project_type,
        p.assetproject_name as name,
        p.assetproject_status,
        p.assetproject_type,
        p.assetproject_priority,
        p.planned_replacement_date,
        p.assetproject_number,
        p.assetproject_category,
        --c.comment as comments,
        p.softdelete_flag
    from 
        {{ ref('rs__ast_project') }} p 


)

select 1 as col from dual
