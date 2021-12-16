 -- depends_on:
    -- {{ ref('dim_assetproject') }}
    -- {{ ref('dim_client') }}
    -- {{ ref('dim_assetzone') }}
    -- {{ ref('dim_assetstatus') }}
    -- {{ ref('dim_materialtype') }}
    -- {{ ref('dim_uniformat') }}
    -- {{ ref('dim_uom') }}
    -- {{ ref('dim_installation_clientperiod') }}
    -- {{ ref('dim_practicallife_replacement_clientperiod') }}
    -- {{ ref('dim_adjusted_practicallife_replacement_clientperiod') }}
    -- {{ ref('dim_meanservicelife_replacement_clientperiod') }}
    -- {{ ref('dim_decommissioned_clientperiod') }}

{{ join_dim(
        from=ref('stg_fact_assetcosts_dim_keys'), 
        relation_alias='fact',
        except=['building_key'])
}}

