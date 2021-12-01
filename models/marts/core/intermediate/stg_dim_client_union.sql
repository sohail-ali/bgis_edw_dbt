{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_client'), ref('stg_dim_client_unknown')]
) }}
