with source as (
    
    select * from {{ source('realsuite', 'client') }}
    
),

renamed as (
    
    select id as client_id,
        name as client_name,
        desciption as description,
        fiscal_year,
        pa_customer_id,
        customer_number,
        class_code,
        status_code,
        pa_organization_id,
        woprefix,
        contract_mgr,
        accounting_org_code,
        theme,
        culture,
        validationcode,
        {{ rs_audit_cols(source('realsuite', 'client')) }},
        {{ cdc_audit_cols(source('realsuite', 'client')) }}
    from source

)

select * from renamed
