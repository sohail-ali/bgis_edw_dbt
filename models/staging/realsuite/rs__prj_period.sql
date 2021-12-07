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
        {{ rs_audit_cols(source('realsuite', 'prj_period')) }},
        {{ cdc_audit_cols(source('realsuite', 'prj_period')) }}
    from source
    where 
        softdelete_flag='N'

)

select * from renamed