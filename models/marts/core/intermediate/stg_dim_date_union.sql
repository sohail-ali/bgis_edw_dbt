{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_date'), ref('stg_dim_date_unknown')]
) }}
