with source as (
    
    select * from {{ source('realsuite', 'ast_project') }}
    
),

renamed as (
    
    select
        asset_project_id,
        client_id,
        asset_project_number as assetproject_number,
        name as assetproject_name,
        status as assetproject_status,
        asset_project_type as assetproject_type,
        date(planned_replacement_date) as planned_replacement_date,
        priority as assetproject_priority,
        sub_program_id,
        capital_project_id,
        createdby,
        createdon as created_date,
        updatedby,
        updatedon as updated_date,
        category as assetproject_category,
        adjustments,
        scope_of_work,
        justification,
        {{ cdc_timestamp_col() }},
        {{ cdc_softdelete_col() }} 
    from source
)

select * from renamed
