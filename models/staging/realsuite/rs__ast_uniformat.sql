with source as (
    select * from {{ source('realsuite', 'ast_uniformat') }}
),

renamed as (
    select
        uniformat_id,
        cast(upper(trim(uniformat_level1)) as varchar(500)) as uniformat_level1,
        cast(upper(trim(uniformat_level2)) as varchar(500)) as uniformat_level2,
        cast(upper(trim(uniformat_level3)) as varchar(500)) as uniformat_level3,
        cast(upper(trim(uniformat_level4)) as varchar(500)) as uniformat_level4,
        material_type,
        mean_service_life,
        practical_life,
        unit_price,
        uom,
        status,
        createdby,
        createdon as created_date,
        updatedby,
        updatedon as updated_date,
        {{ cdc_timestamp_col() }},
        {{ cdc_softdelete_col() }} 
    from source
)

select * from renamed