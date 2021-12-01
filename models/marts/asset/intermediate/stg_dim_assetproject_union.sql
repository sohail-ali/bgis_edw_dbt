{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_assetproject'), ref('stg_dim_assetproject_unknown')]
) }}
