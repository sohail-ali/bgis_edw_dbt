with unknown_clientfiscalperiod as (
    select 
        cast(0 as number) as clientfiscalperiod_key,
        cast(-1 as number) as client_id,
        to_date('1900-01-01') AS thedate,
        cast(0 as number)   AS fiscal_year,
        cast(0 as number) AS fiscal_quarter,
        cast(0 as number)  AS fiscal_month,
        FALSE AS fiscalmonth_open_ind,
        cast('N/A' as varchar(500)) AS fiscal_month_name,
        to_date('1900-01-01') AS fiscal_start_date,
        to_date('1900-01-01') AS fiscal_end_date,
        cast(0 as number) AS accounting_year,
        cast(0 as number) AS accounting_quarter,
        cast(0 as number)  AS accounting_month,
        cast(NULL AS varchar(500)) AS accounting_month_name,
        FALSE AS accountingmonth_open_ind,
        to_date('1900-01-01') AS accounting_start_date,
        to_date('1900-01-01') AS accounting_end_date,
        FALSE AS softlock_ind,
        cast('PAST' as varchar(50)) AS fiscalmonth_type,
        cast('PAST' as varchar(50)) AS fiscalyear_type,
        cast('PAST' as varchar(50)) AS accountingmonth_type,
        cast('PAST' as varchar(50)) AS accountingyear_type,
        cast('N' as char(1)) AS softdelete_flag
    from DUAL
)

select * from unknown_clientfiscalperiod
