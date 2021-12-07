with source as (
    
    select * from {{ source('realsuite', 'ast_comment_item') }}
    
),

renamed as (
    
    select
        comment_item_id,
        object_class,
        object_id,
        note,
        {{ rs_audit_cols(source('realsuite', 'ast_comment_item')) }},
        {{ cdc_audit_cols(source('realsuite', 'ast_comment_item')) }}
    from source
    where 
        softdelete_flag='N'
)

select * from renamed
