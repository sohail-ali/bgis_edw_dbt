with dim_clientperiod_unknown as 
(
select 
    cast(0 as number) as clientperiod_key,
    cast(-1 as number) as client_id,
    cast(0 as number) as fiscal_year,
    cast(0 as number) as fiscal_quarter,
    cast(0 as number) as fiscal_month_number,
    FALSE as fiscal_openmonth_ind,
    to_date('1900-01-01') as fiscal_month_start_date,
    to_date('1900-01-01') as fiscal_month_end_date,
    cast(0 as number) as accounting_year,
    cast(0 as number) as accounting_quarter,
    cast(0 as number) as accounting_month_number,
    FALSE as accounting_openmonth_ind,
    to_date('1900-01-01') as accounting_month_start_date,
    to_date('1900-01-01') as accounting_month_end_date,
    cast('JAN-1900' as varchar(500)) as full_month_name,
    cast('N/A' as varchar(500)) as month_name,
    cast('PAST' as varchar(50)) as fiscalmonth_type,
    cast('PAST' as varchar(50)) as fiscalyear_type,
    cast('PAST' as varchar(50)) as accountingmonth_type,
    cast('PAST' as varchar(50)) as accountingyear_type,    
    cast('N' as char(1)) as softdelete_flag,
    FALSE as softlock_ind,
    cast(NULL as date) softlock_start_date,
    cast(NULL as date) softlock_end_date
from dual
)

select * from dim_clientperiod_unknown