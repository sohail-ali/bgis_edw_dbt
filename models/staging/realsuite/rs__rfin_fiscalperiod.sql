with source as (
    
    select * from {{ source('realsuite', 'rfin_fiscalperiod') }}
),

renamed as (
    
    select
        fiscalperiod_id,
        fiscalyear_id,
        client_id,
        upper(trim(period_name)) as period_name,
        period_type,
        cast(period_num as number(18)) as period_num,
        to_date(quarter_effdate) as quarter_effdate,
        cast(quarter_num as number(18)) as quarter_num,
        entered_periodname,
        upper(trim(status)) as status,
        to_date(effective_startdate) as effective_startdate,
        to_date(effective_enddate) as effective_enddate,
        to_date(accounting_period_startdate) as accounting_period_startdate,
        to_date(accounting_period_enddate) as accounting_period_enddate,
        to_date(rprj_forecast_startdate) as rprj_forecast_startdate,
        to_date(rprj_forecast_enddate) as rprj_forecast_enddate,
        {{ rs_audit_cols(source('realsuite', 'rfin_fiscalperiod')) }},
        {{ cdc_audit_cols(source('realsuite', 'rfin_fiscalperiod')) }}
    from source
)

select * from renamed