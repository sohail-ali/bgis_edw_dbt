{{ dbt_utils.union_relations(
    relations=[ ref('stg_dim_clientfiscalperiod'), ref('stg_dim_clientfiscalperiod_unknown')]
) }}
