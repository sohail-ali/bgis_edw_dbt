with 
seq_number as (
    {{ generate_series(upper_bound=100) }}
)

, fact_activeassetcosts as (
    select
        asset_id,
        building_item_number,
        asset_project_id,
        a.client_id,
        building_id,
        uniformat_level1,
        uniformat_level2,
        uniformat_level3,
        uniformat_level4,
        status,
        material_type,
        uom,
        assetzone_id,
        practical_life,
        practical_life_adjustment,
        mean_service_life,
        replacement_cost,
        case when generated_number=1 then 
            adjusted_replacement_cost 
            else replacement_cost end as adjusted_replacement_cost,
        case when generated_number=1 then 
            field_replacement_estimate_cost 
            else NULL end as field_replacement_estimate_cost,
        case when generated_number=1 then 
            practicallife_replacement_date 
            else            
                case when practicallife_replacement_date < current_date then 
                    dateadd (year,
                            ((floor((year(current_date)-year(practicallife_replacement_date))/practical_life)+1)*practical_life) + 
                            (practical_life*(generated_number-2))
                            ,practicallife_replacement_date)
                else dateadd(year,practical_life*(generated_number-1),practicallife_replacement_date) end 
            end as practicallife_replacement_date,
        case when generated_number = 1 then 
            adjusted_practicallife_replacement_date 
            else 
                case when adjusted_practicallife_replacement_date < current_date then 
                        dateadd (year,
                                ((floor((year(current_date) - year(adjusted_practicallife_replacement_date))/practical_life)+1) * practical_life) + 
                                (practical_life*(generated_number-2)),
                                adjusted_practicallife_replacement_date) 
                    else dateadd(year,practical_life*(generated_number-1),adjusted_practicallife_replacement_date) end 
            end as adjusted_practicallife_replacement_date,
        case when generated_number = 1 then 
            meanservicelife_replacement_date 
            else 
                case when meanservicelife_replacement_date < current_date then 
                        dateadd (year,
                                ((floor((year(current_date) - year(meanservicelife_replacement_date))/mean_service_life)+1) * mean_service_life) + 
                                (mean_service_life*(generated_number-2)),
                                meanservicelife_replacement_date) 
                    else dateadd(year,mean_service_life*(generated_number-1),meanservicelife_replacement_date) end 
            end as meanservicelife_replacement_date,
        generated_number as step,
        forecast_end_date
    from 
        {{ ref('stg_fact_assetcosts') }} a
    join {{ref('stg_wrk_asset_forecast') }} f on 
        a.client_id = f.client_id
    cross join seq_number 
    where
        decomissioned_date is null  
)
select * from fact_activeassetcosts
