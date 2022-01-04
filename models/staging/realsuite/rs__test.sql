with source as (

    select * from {{ source('realsuite', 'client') }}

),



renamed as (

    select
        id as id,
        name as name,
        desciption as desciption,
        fiscal_year as fiscal_year,
        pa_customer_id as pa_customer_id,
        customer_number as customer_number,
        class_code as class_code,
        status_code as status_code,
        pa_organization_id as pa_organization_id,
        woprefix as woprefix,
        contract_mgr as contract_mgr,
        created_on as created_on,
        created_by as created_by,
        updated_on as updated_on,
        updated_by as updated_by,
        accounting_org_code as accounting_org_code,
        theme as theme,
        culture as culture,
        validationcode as validationcode,
        cdc_modified_datetime as cdc_modified_datetime,
        cast(case when ifnull(cdc_softdelete_flag,'') ='D' then 'Y' else 'N' end as char(1)) as softdelete_flag,
        dss_record_source as dss_record_source,
        dss_load_date as dss_load_date,
        dss_create_time as dss_create_time,
        dss_update_time as dss_update_time

    from source

)

select * from renamed
