with source as (
    
    select * from {{ source('realsuite', 'rfin_moduleoption') }}
    
),

renamed as (
    
    select
        moduleoption_id,
        client_id,
        weather_normalization,
        cdc_modified_datetime,
        decode(ifnull(cdc_softdelete_flag,''),'D','Y','N') as cdc_softdelete_flag
    from source

)

select * from renamed
