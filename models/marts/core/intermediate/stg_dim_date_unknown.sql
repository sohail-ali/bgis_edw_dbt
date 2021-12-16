{{ unknown_member(from=ref('stg_dim_date'),
                    except=["fiscalyear","fiscalquarter","fiscalmonth",
                            "fiscalmonth_name","fiscalweek","fiscalweek_name",
                            "month_date"],
                    override=[
                              "cast(1900 as number) as calendaryear",
                              "cast(2 as number) as dayofweek",
                              "cast('Q1-1900' as varchar(500)) as year_quarter",
                              "cast('WK1-1900' as varchar(500)) as year_week",
                              "cast('JAN-1900' as varchar(500)) as year_month",
                              "cast('PAST' as varchar(50)) as accountingyear_type",
                              "cast('January' as varchar(500)) as month_name",
                              "cast('Q1' as varchar(500)) as quarter_name"
                              ]) }}
                              
