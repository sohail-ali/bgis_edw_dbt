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
        {{ cdc_timestamp_col() }}
    from source
    where {{ cdc_softdelete_filter() }} 

)

select * from renamed
