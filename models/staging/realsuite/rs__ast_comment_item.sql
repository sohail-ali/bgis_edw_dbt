with source as (
    
    select * from {{ source('realsuite', 'ast_comment_item') }}
    
),

renamed as (
    
    select
        comment_item_id,
        object_class,
        object_id,
        note,
        createby as createdby,
        createdon as created_date,
        {{ cdc_timestamp_col() }}
    from source
    where 
        {{ cdc_softdelete_filter() }} 
)

select * from renamed
