{{ unknown_member(from=ref('stg_dim_clientperiod'),
                    except=["accounting_month_name","softlock_start_date","softlock_end_date","month_name"],
                    override=[
                              "cast('JAN-1900' as varchar(500)) as full_month_name",
                              "cast('PAST' as varchar(50)) AS fiscalmonth_type",
                              "cast('PAST' as varchar(50)) AS fiscalyear_type",
                              "cast('PAST' as varchar(50)) AS accountingmonth_type",
                              "cast('PAST' as varchar(50)) AS accountingyear_type"
                              ]) }}
                              