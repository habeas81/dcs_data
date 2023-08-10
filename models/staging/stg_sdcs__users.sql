with

source as (
    select * from {{ source('sdcs', 'user' )}}
)


clean as (
    select
        id as user_id,
        name,
        first_seen,
        last_seen
)

final as (
    select * from clean
)

select * from final
