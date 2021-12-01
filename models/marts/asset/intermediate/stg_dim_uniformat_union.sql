{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_uniformat'), ref('stg_dim_uniformat_unknown')]
) }}
