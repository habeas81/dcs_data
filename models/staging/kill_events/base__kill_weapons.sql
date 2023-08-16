{{
    config(
        materialized="ephemeral"
    )
}}
with

kills as (
    select * from {{ ref('base__kill_events') }}
),

columns as (
    select
        kill_id,
        weapon
    from kills
),

flatten as (
    select
        kill_id,
        (weapon ->> 'id') as weapon_id,
        weapon ->> 'type' as weapon_type,
        (weapon -> 'position' ->> 'u')::float as weapon_position_u,
        (weapon -> 'position' ->> 'v')::float as weapon_position_v,
        (weapon -> 'position' ->> 'alt')::float as weapon_altitude,
        (weapon -> 'position' ->> 'lat')::float as weapon_latitude,
        (weapon -> 'position' ->> 'lon')::float as weapon_longitude,
        (weapon -> 'velocity' ->> 'speed')::float as weapon_speed,
        (weapon -> 'velocity' ->> 'heading')::float as weapon_heading
    from columns
),

filtered as (
    select
        *
    from flatten
    where NOT flatten.weapon_id IS NULL
),

final as (
    select * from filtered
)

select * from final
