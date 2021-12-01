with dim_assetstatus as (
    select distinct 
         cast(hash(upper(status)) as number(38)) as assetstatus_key,
         cast(upper(status) as varchar(500)) as status
    from {{ ref('rs__equipment')}} 
    where 
        softdelete_flag='N'
)

select * from dim_assetstatus
