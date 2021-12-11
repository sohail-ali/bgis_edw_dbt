{{ unknown_member(from=ref('stg_dim_client'),
                    except=["pds_client_flag"],
                    override=["cast(TRUE as boolean) as lease_exclusion_flag"]) }}
