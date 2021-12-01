with date_seq as (
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('1970-01-01' as date)",
    end_date="cast('2100-01-01' as date)"
   )
}}
)

, gl_month as (
    select 
         period_year AS fiscalyear
       , quarter_num AS fiscalquarter
       , period_num AS fiscalmonth
       , period_name AS fiscalmonth_name
       , start_date
       , end_date
    from {{ ref('ebs__gl_periods_corp')}}
    where 
        period_type ='Month'
)

, gl_week as (
    select 
         period_num AS fiscalweek
       , period_name AS fiscalweek_name
       , start_date
       , end_date
    from {{ ref('ebs__gl_periods_corp')}}
    where 
        period_type ='1'
)

, date_calendar as (
    select 
        date_day as thedate
      , cast(to_char(thedate,'yyyymmdd') as number) AS date_key
      , year(thedate) AS calendaryear
      , quarter(thedate) AS calendarquarter
      , month(thedate) AS calendarmonth
      , week(thedate) AS calendarweek
      , dayofweek(thedate) AS dayofweek
      , 'Q'|| quarter(thedate) ||'-' || year(thedate) AS year_quarter
      , 'WK'|| week(thedate) ||'-' || year(thedate) AS year_week
      , upper(monthname(thedate))||'-' || year(thedate) AS year_month
      , dayofmonth(thedate) AS dayofmonth
      , dayofyear(thedate) AS dayofyear
      , cast(to_char(thedate,'mmdd') as number) AS month_date
      , to_char(thedate,'MMMM') AS month_name
      , case quarter(thedate) when 1 then 'First Quarter' when 2 then 'Second Quarter' when 3 then 'Third Quarter' else 'Forth Quarter' end AS quarter_name
    from date_seq 
)

, dim_date as (
    select   
          date_key
        , thedate
        , calendaryear
        , calendarquarter
        , calendarmonth
        , calendarweek
        , dayofweek
        , year_quarter
        , year_week
        , year_month
        , dayofmonth
        , dayofyear
        , month_date
        , month_name
        , quarter_name
        , gl_month.fiscalyear
        , gl_month.fiscalquarter
        , gl_month.fiscalmonth
        , gl_month.fiscalmonth_name
        , gl_week.fiscalweek
        , gl_week.fiscalweek_name
  from date_calendar
  left join gl_month on 
    thedate between gl_month.start_date and gl_month.end_date 
  left join gl_week on 
    thedate between gl_week.start_date and gl_week.end_date 
)

, unknown_date as (
    select 0 as date_key 
        , date('1900-01-01') AS thedate
        , 1900 AS calendaryear
        , 1 AS calendarquarter
        , 1 AS calendarmonth
        , 1 AS calendarweek
        , 2 AS dayofweek
        , SUBSTRING('Q1-1900',1,500) AS year_quarter
        , SUBSTRING('WK1-1900',1,500) AS year_week
        , SUBSTRING('JAN-1900',1,500) AS year_month
        , 1 AS dayofmonth
        , 1 AS dayofyear
        , CAST(NULL AS number) AS month_date
        , SUBSTRING('January',1,500) AS month_name
        , SUBSTRING('Q1',1,500) AS quarter_name
        , CAST(NULL AS number) AS fiscalyear
        , CAST(NULL AS number) AS fiscalquarter
        , CAST(NULL AS number) AS fiscalmonth
        , CAST(NULL AS varchar(500)) AS fiscalmonth_name
        , CAST(NULL AS number) AS fiscalweek
        , CAST(NULL AS varchar(500)) AS fiscalweek_name    
    from dual
)

    select   
          date_key
        , thedate
        , calendaryear
        , calendarquarter
        , calendarmonth
        , calendarweek
        , dayofweek
        , year_quarter
        , year_week
        , year_month
        , dayofmonth
        , dayofyear
        , month_date
        , month_name
        , quarter_name
        , fiscalyear
        , fiscalquarter
        , fiscalmonth
        , fiscalmonth_name
        , fiscalweek
        , fiscalweek_name
    from dim_date
    union all 
        select   
          date_key
        , thedate
        , calendaryear
        , calendarquarter
        , calendarmonth
        , calendarweek
        , dayofweek
        , year_quarter
        , year_week
        , year_month
        , dayofmonth
        , dayofyear
        , month_date
        , month_name
        , quarter_name
        , fiscalyear
        , fiscalquarter
        , fiscalmonth
        , fiscalmonth_name
        , fiscalweek
        , fiscalweek_name
    from unknown_date
    order by 1
    

