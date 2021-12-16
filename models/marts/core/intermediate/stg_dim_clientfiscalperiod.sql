with 
  seq_number as (
    {{ dbt_utils.generate_series(upper_bound=100) }}
)

, clientperiod as (
    select client_id,
      dateadd(day, generated_number-1, fiscal_month_start_date) AS thedate,
      fiscal_year,
      fiscal_quarter,
      fiscal_month_number as fiscal_month,
      month_name as fiscal_month_name,
      fiscal_openmonth_ind as fiscalmonth_open_ind,
      fiscal_month_start_date as fiscal_start_date,
      fiscal_month_end_date as fiscal_end_date,
      fiscalmonth_type,
      fiscalyear_type,
      case 
        when thedate between softlock_start_date and softlock_end_date and softlock_ind= TRUE then TRUE 
        else FALSE end as softlock_ind,
      softdelete_flag
    from {{ ref('stg_dim_clientperiod')}} 
    cross join  seq_number 
    where 
    client_id <> -1
    and thedate between fiscal_month_start_date and  fiscal_month_end_date 
)
, clientperiod_accounting as (
    select client_id,
        accounting_year,
        accounting_quarter,
        accounting_month_number as accounting_month,
        month_name as accounting_month_name,
        accounting_openmonth_ind as accountingmonth_open_ind,
        accounting_month_start_date as accounting_start_date,
        accounting_month_end_date as accounting_end_date,
        accountingyear_type,
        accountingmonth_type
    from {{ ref('stg_dim_clientperiod')}} 
    where 
        client_id <> -1
        and accounting_month_start_date is not null
        and accounting_month_end_date is not null
)

, dim_clientfiscalperiod as (
    select 
      {{ surrogate_key('cp.client_id','cp.thedate') }} as clientfiscalperiod_key,
      cp.client_id,
      cp.thedate,
      cp.fiscal_year,
      cp.fiscal_quarter,
      cp.fiscal_month,
      cp.fiscal_month_name,
      cp.fiscalmonth_open_ind,
      cp.fiscal_start_date,
      cp.fiscal_end_date,
      a.accounting_year,
      a.accounting_quarter,
      a.accounting_month,
      a.accounting_month_name,
      a.accountingmonth_open_ind,
      a.accounting_start_date,
      a.accounting_end_date,
      cp.softlock_ind,
      cp.fiscalmonth_type,
      cp.fiscalyear_type,
      a.accountingmonth_type,
      a.accountingyear_type,
      cp.softdelete_flag
    from 
    clientperiod cp
    left join clientperiod_accounting a on 
      cp.client_id=a.client_id
      and cp.thedate between a.accounting_start_date and a.accounting_end_date
)

select * from dim_clientfiscalperiod
