{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_clientperiod'), ref('stg_dim_clientperiod_unknown')]
) }}
