with source as (
    
    select * from {{ source('realsuite', 'lookup_code') }}
    
),

renamed as (
    
    select
        lookup_code_id,
        upper(lookup_type_class) as lookup_type_class,
        lookup_value,
        lookup_definition,
        client_id,
        default_flag,
        system_code,
        lookup_sortcode,    
        created_on as created_date,
        created_by,
        updated_on as updated_date,
        updated_by,
        {{ cdc_timestamp_col() }}
    from source
    where {{ cdc_softdelete_filter() }} 

)

select * from renamed
