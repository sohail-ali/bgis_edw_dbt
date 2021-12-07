with source as (
    
    select * from {{ source('realsuite', 'rfin_moduleoption') }}
    
),

renamed as (
    
    select
        moduleoption_id,
        client_id,
        weather_normalization,
        {{ cdc_audit_cols(source('realsuite', 'rfin_moduleoption')) }}
    from source
    where 
      softdelete_flag='N'
)

select * from renamed
