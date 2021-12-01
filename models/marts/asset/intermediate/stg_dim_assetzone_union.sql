{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_assetzone'), ref('stg_dim_assetzone_unknown')]
) }}
