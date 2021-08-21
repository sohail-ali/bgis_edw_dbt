with source as (
    
    select * from {{ source('realsuite', 'client') }}
    
),

renamed as (
    
    select
        id as client_id,
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
        created_on,
        created_by,
        updated_on,
        updated_by,
        accounting_org_code,
        theme,
        culture,
        validationcode,
        cdc_modified_datetime,
        decode(ifnull(cdc_softdelete_flag,''),'D','Y','N') as cdc_softdelete_flag
    from source

)

select * from renamed
