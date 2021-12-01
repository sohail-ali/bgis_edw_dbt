{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_uom'), ref('stg_dim_uom_unknown')]
) }}
