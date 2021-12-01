with unknown_date as (
    select cast(0 as number) as date_key 
        , date('1900-01-01') AS thedate
        , cast(1900 as number) AS calendaryear
        , cast(1 as number) AS calendarquarter
        , cast(1 as number) AS calendarmonth
        , cast(1 as number) AS calendarweek
        , cast(2 as number) AS dayofweek
        , cast(SUBSTRING('Q1-1900',1,500) as varchar(500)) AS year_quarter
        , cast(SUBSTRING('WK1-1900',1,500) as varchar(500)) AS year_week
        , cast(SUBSTRING('JAN-1900',1,500) as varchar(500)) AS year_month
        , cast(1 as number) AS dayofmonth
        , cast(1 as number) AS dayofyear
        , cast(NULL AS number) AS month_date
        , cast(SUBSTRING('January',1,500) as varchar(500)) AS month_name
        , cast(SUBSTRING('Q1',1,500) as varchar(500)) AS quarter_name
        , cast(NULL AS number) AS fiscalyear
        , cast(NULL AS number) AS fiscalquarter
        , cast(NULL AS number) AS fiscalmonth
        , cast(NULL AS varchar(500)) AS fiscalmonth_name
        , cast(NULL AS number) AS fiscalweek
        , cast(NULL AS varchar(500)) AS fiscalweek_name    
    from dual
)

select * from unknown_date

