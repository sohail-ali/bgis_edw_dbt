{{ config(
  enabled=false
) }}

{{ depends_on_dim(
        from=ref('stg_fact_activeassetcosts_dim_keys')
        )
}}
