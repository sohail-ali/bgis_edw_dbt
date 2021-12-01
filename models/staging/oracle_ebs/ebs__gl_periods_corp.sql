with source as (
    
    select * from {{ source('oracle_ebs', 'gl_periods') }}
    
),

renamed as (
    
    select
        period_set_name,
        period_name,
        start_date,
        end_date,
        period_type,
        period_year,
        period_num,
        quarter_num,
        entered_period_name
    from source
    where
     start_date < end_date 
     and upper(period_set_name)='CORP CALENDAR'

)

select * from renamed
