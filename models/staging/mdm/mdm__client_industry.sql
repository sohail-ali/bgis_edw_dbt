with source as (
    
    select * from {{ source('mdm', 'client_industry') }}
    
),

renamed as (
    
    select
        client_id,
        industry_name,
        industry_group_code,
        benchmark_flag,
        dss_create_time,
        dss_update_time
    from source
)

select * from renamed
