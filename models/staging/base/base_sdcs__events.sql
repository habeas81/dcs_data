with

source as (
    select * from {{ source('sdcs', 'events')}}
),

rename as (
    -- ids
    select 
    id as event_id,
    campaign_id,
    initiator_user_id,
    target_user_id,

    -- time
    time as time_created,

    -- data
    data

    from source
),

final as (
    select * from source
)

select * from final
