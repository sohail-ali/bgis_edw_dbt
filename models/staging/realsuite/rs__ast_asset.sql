with source as (
    
    select * from {{ source('realsuite', 'ast_asset') }}
    
),

renamed as (
    
    select
        asset_id,
        client_id,
        uniformat_id,
        cast(quantity as number(38)) as quantity,
        ifnull(replacement_responsibility,'N') as replacement_responsibility,
        is_estimated_inst_date,
        ifnull(practical_life_adjustment,0) as practical_life_adjustment,
        field_replacement_estimate,
        date(field_estimate_date) as field_estimate_date ,
        asset_project_id,
        age_vs_service_life,
        condition,
        oper_critclty,
        impact_of_failure,
        prob_attr,
        impt_attr,
        aggr_scr,
        additional_zone_info,
        responsibility_other,
        action_category,
        capital_planning,
        {{ rs_audit_col() }},
        {{ cdc_timestamp_col() }},
        {{ cdc_softdelete_col() }} 
    from source

)

select * from renamed
