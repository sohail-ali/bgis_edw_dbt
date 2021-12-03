with ast_comment as (
    select 
        object_id,
        object_class,
        cast('Note_'||  row_number() over 
                (partition by object_class,object_id order by created_date desc)  as varchar(500)) as comment_rank,
        note
    from {{ ref('rs__ast_comment_item')}} 
    where 
        object_class in ('AST_PROJECT','AST_ASSET','AST_RECOMMENDED_ACTION')
)
, ast_comment_pivot as (
    select
        object_id, 
        object_class,
        cast( 
            decode(Note_1,NULL,'',Note_1||CHR(13)||CHR(10)) || 
            decode(Note_2,NULL,'',Note_2||CHR(13)||CHR(10)) || 
            decode(Note_3,NULL,'',Note_3) as varchar(6000)) as comment
    FROM ast_comment
    pivot (max(note) for comment_rank in ('Note_1','Note_2','Note_3')) as p 
    (object_id,object_class, Note_1, Note_2, Note_3)

)

select * from ast_comment_pivot