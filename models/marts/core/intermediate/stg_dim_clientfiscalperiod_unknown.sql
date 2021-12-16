{{ unknown_member(from=ref('stg_dim_clientfiscalperiod'),
                    except=["accounting_month_name"],
                    override=["cast('PAST' as varchar(50)) AS fiscalmonth_type",
                              "cast('PAST' as varchar(50)) AS fiscalyear_type",
                              "cast('PAST' as varchar(50)) AS accountingmonth_type",
                              "cast('PAST' as varchar(50)) AS accountingyear_type"
                              ]) }}
                              