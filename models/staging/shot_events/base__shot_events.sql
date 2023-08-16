{{
    config(
        materialized="view"
    )
}}
with

events as (
    select * from {{ ref('base_sdcs__events') }}
),

shots as (
    select * from events where events.data ->> 'event' = 'shot'
),

split as (
    select
    event_id as shot_id,
    campaign_id,
    initiator_user_id,
    target_user_id,
    time_created,
    data -> 'shot' ->> 'weaponName' as weapon_name,
    data -> 'shot' -> 'target' as target,
    data -> 'shot' -> 'initiator' as initiator,
    data -> 'shot' -> 'weapon' as weapon

    from shots
),

final as (
    select * from split
)

select * from final
