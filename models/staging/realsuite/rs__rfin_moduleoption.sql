with source as (
    
    select * from {{ source('realsuite', 'rfin_moduleoption') }}
    
),

renamed as (
    
    select
        moduleoption_id,
        client_id,
        weather_normalization,
        {{ cdc_timestamp_col() }}
    from source
    where 
      {{ cdc_softdelete_filter() }}
)

select * from renamed
