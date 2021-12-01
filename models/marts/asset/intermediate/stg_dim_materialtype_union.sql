{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_materialtype'), ref('stg_dim_materialtype_unknown')]
) }}
