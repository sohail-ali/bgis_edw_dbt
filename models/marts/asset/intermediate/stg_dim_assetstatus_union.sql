{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_assetstatus'), ref('stg_dim_assetstatus_unknown')]
) }}
