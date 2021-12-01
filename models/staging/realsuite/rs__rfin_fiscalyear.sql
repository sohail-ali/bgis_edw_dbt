with source as (
    
    select * from {{ source('realsuite', 'rfin_fiscalyear') }}
    
),

renamed as (
    
    select
        fiscalyear_id,
        client_id,
        status,
        to_date(effective_startdate) as effective_startdate,
        to_date(effective_enddate) as effective_enddate,
        fiscal_firstmonth,
        cast(display_year as number(18)) as display_year,
        to_date(last_closedate) as last_closedate,
        last_closeuser,
        to_date(last_reopendate) as last_reopendate,
        to_date(rprj_plan_startdate) as rprj_plan_startdate,
        to_date(rprj_plan_enddate) as rprj_plan_enddate,
        {{ cdc_timestamp_col() }}
    from source
    where 
        {{ cdc_softdelete_filter() }}
)

select * from renamed