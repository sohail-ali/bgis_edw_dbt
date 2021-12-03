with 
curr_accounting_period as (
    select p.client_id,
      max(to_date(display_year||'-'||cast(period_num as int),'YYYY-MM')) as curr_accounting_month
    from 
    {{ ref('rs__rfin_fiscalperiod') }} p
    join {{ ref('rs__rfin_fiscalyear') }} y on 
      p.fiscalyear_id=y.fiscalyear_id
    where
      p.status='O'
      and p.softdelete_flag='N'
    group by 1
)

, curr_accounting_year as (
    select client_id,
      max(display_year) as curr_accounting_year
    from 
    {{ ref('rs__rfin_fiscalyear') }} 
    where
      status='O'
    group by 1
)

,rfin_fiscalyear as (
    select fiscalyear_id,
        display_year as fiscal_year,
        display_year as accounting_year,
        case
            when current_date between effective_startdate and effective_enddate then  'CURRENT'
            when add_months(current_date,-12) between effective_startdate and effective_enddate then 'PREVIOUS'
            when add_months(current_date,12) between effective_startdate and effective_enddate then 'NEXT'
            when effective_startdate > add_months(current_date,12) then 'FUTURE'
            else 'PAST'end  fiscalyear_type,
        case 
            when accounting_year = curr_accounting_year   then 'CURRENT'
            when accounting_year = curr_accounting_year-1 then 'PREVIOUS'
            when accounting_year < curr_accounting_year-1 then 'PAST'
            when accounting_year = curr_accounting_year+1 then 'NEXT'
            when accounting_year > curr_accounting_year+1 then 'FUTURE'
            else NULL end as accountingyear_type
    from 
    {{ ref('rs__rfin_fiscalyear') }} y
    left join curr_accounting_year c on 
      y.client_id = c.client_id
)

, softlock_period as (
    select fiscalperiod_id,
        softlock_start_date,
        softlock_end_date,
        case when current_date between softlock_start_date and softlock_end_date then TRUE 
            else FALSE 
            end as softlock_ind 
    from 
    {{ ref('rs__prj_period') }}
    qualify 
        row_number() over (partition by fiscalperiod_id order by updated_date desc)=1

)
, rfin_fiscalperiod as (
    select 
      p.fiscalperiod_id,
      p.softdelete_flag,
      p.fiscalyear_id,
      p.client_id,
      y.fiscal_year,
      p.quarter_num as fiscal_quarter,
      p.period_num as fiscal_month_number,
      case when current_date between p.effective_startdate and p.effective_enddate then TRUE else FALSE end AS fiscal_openmonth_ind,
      p.effective_startdate AS fiscal_month_start_date,
      p.effective_enddate AS fiscal_month_end_date,
      y.fiscalyear_type,
      case
        when current_date between p.effective_startdate and p.effective_enddate then  'CURRENT'
        when add_months(current_date,-1) between p.effective_startdate and p.effective_enddate then 'PREVIOUS'
        when add_months(current_date,1) between p.effective_startdate and p.effective_enddate then 'NEXT'
        when p.effective_startdate > add_months(current_date,1) then 'FUTURE'
        else 'PAST' end AS fiscalmonth_type,
      CAST(LEFT(p.period_name,4)|| CAST(YEAR(p.effective_enddate) AS CHAR(10)) as VARCHAR(50)) AS full_month_name,
      p.period_name AS month_name,
      y.accounting_year,
      p.quarter_num as accounting_quarter,
      p.period_num as accounting_month_number,
      to_date(y.accounting_year||'-'||cast(p.period_num as int),'YYYY-MM') as accounting_month,
      y.accountingyear_type,
      case 
        when curr_accounting_month = accounting_month then 'CURRENT'
        when add_months(curr_accounting_month,-1) = accounting_month then 'PREVIOUS'
        when add_months(curr_accounting_month,-1) > accounting_month then 'PAST'
        when add_months(curr_accounting_month,1) = accounting_month then 'NEXT'
        when add_months(curr_accounting_month,1) < accounting_month then 'FUTURE'
        else NULL end as accountingmonth_type,
      case when p.status='O' then TRUE else FALSE end AS accounting_openmonth_ind,
      p.accounting_period_startdate as accounting_month_start_date,
      p.accounting_period_enddate AS accounting_month_end_date
    from 
    {{ ref('rs__rfin_fiscalperiod') }} p
    join rfin_fiscalyear y on 
      p.fiscalyear_id=y.fiscalyear_id
    left join curr_accounting_period c on 
      p.client_id= c.client_id
)

, dim_clientperiod as (
    select 
      {{ surrogate_key('client_id','full_month_name') }} as clientperiod_key,
      client_id,
      cast(fiscal_year as number) as fiscal_year,
      cast(fiscal_quarter as number) as fiscal_quarter,
      cast(fiscal_month_number as number) as fiscal_month_number,
      fiscal_openmonth_ind,
      fiscal_month_start_date,
      fiscal_month_end_date,
      cast(accounting_year as number) as accounting_year,
      cast(accounting_quarter as number) as accounting_quarter,
      cast(accounting_month_number as number) as accounting_month_number,
      accounting_openmonth_ind,
      accounting_month_start_date,
      accounting_month_end_date,
      cast(full_month_name as varchar(500)) as full_month_name,
      cast(month_name as varchar(500)) as month_name,
      cast(fiscalmonth_type as varchar(50)) as fiscalmonth_type,
      cast(fiscalyear_type as varchar(50)) as fiscalyear_type,
      cast(accountingmonth_type as varchar(50)) as accountingmonth_type,
      cast(accountingyear_type as varchar(50)) as accountingyear_type,    
      softdelete_flag,
      IFNULL(softlock_ind,FALSE) as softlock_ind ,
      softlock_start_date,
      softlock_end_date
    from rfin_fiscalperiod p
    left join softlock_period s on 
      p.fiscalperiod_id=s.fiscalperiod_id
)

select * from dim_clientperiod


