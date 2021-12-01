with source as (
    select * from {{ source('realsuite', 'prj_period') }}
),

renamed as (
    
    select
        period_id,
        client_id,
        fiscalyear_id,
        fiscalperiod_id,
        to_date(softlock_start_date) as softlock_start_date,
        to_date(softlock_end_date) as softlock_end_date,
        updatedon as updated_date,
        {{ cdc_timestamp_col() }}
    from source
    where 
        {{ cdc_softdelete_filter() }}

)

select * from renamed