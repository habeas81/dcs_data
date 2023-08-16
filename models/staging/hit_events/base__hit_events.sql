{{
    config(
        materialized="ephemeral"
    )
}}
with

events as (
    select * from {{ ref('base_sdcs__events') }}
),

hits as (
    select * from events where events.data ->> 'event' = 'hit'
),

split as (
    select
    event_id as hit_id,
    campaign_id,
    initiator_user_id,
    target_user_id,
    time_created,
    data -> 'hit' ->> 'weaponName' as weapon_name,
    data -> 'hit' -> 'target' as target,
    data -> 'hit' -> 'initiator' as initiator,
    data -> 'hit' -> 'weapon' as weapon

    from hits
),

final as (
    select * from split
)

select * from final
