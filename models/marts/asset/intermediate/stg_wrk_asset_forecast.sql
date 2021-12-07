with asset_forecat as (
    select
        cp.client_id,
        max(cp.fiscal_month_end_date) as forecast_end_date
    from 
    {{ ref('dim_clientperiod') }} cp
    join {{ ref('dim_clientperiod') }} cur_cp
        on cp.client_id = cur_cp.client_id
    where 
        cp.fiscal_year <= cur_cp.fiscal_year + 30
        and cur_cp.full_month_name = {{ full_month_name('current_date') }}
        and cur_cp.client_id > 0
    group by 1
)

select * from asset_forecat