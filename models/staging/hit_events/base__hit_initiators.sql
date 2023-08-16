{{
    config(
        materialized="ephemeral"
    )
}}
with


hits as (
    select * from {{ ref('base__hit_events') }}
),

columns as (
    select
        hit_id,
        initiator ->> 'initiator' as initiator_category,
        initiator
    from hits
),

units as (
    select 
        hit_id,
        initiator_category,
        (initiator -> 'unit' ->> 'id')::int as initiator_id,
        initiator -> 'unit' ->> 'name' as initiator_name,
        initiator -> 'unit' ->> 'type' as initiator_type,
        initiator -> 'unit' ->> 'callsign' as initiator_callsign,
        initiator -> 'unit' ->> 'coalition' as initiator_coalition,
        (initiator -> 'unit' -> 'position' -> 'u')::float as initiator_position_u,
        (initiator -> 'unit' -> 'position' -> 'v')::float as initiator_position_v,
        (initiator -> 'unit' -> 'position' -> 'lat')::float as initiator_latitude,
        (initiator -> 'unit' -> 'position' -> 'lon')::float as initiator_longitude,
        (initiator -> 'unit' -> 'position' -> 'alt')::float as initiator_altitude,
        (initiator -> 'unit' -> 'velocity' -> 'speed')::float as initiator_speed,
        (initiator -> 'unit' -> 'group' ->> 'id')::int as initiator_group_id,
        initiator -> 'unit' -> 'group' ->> 'name' as initiator_group_name,
        initiator -> 'unit' -> 'group' ->> 'category' as initiator_group_category,
        initiator -> 'unit' -> 'group' ->> 'coalition' as initiator_group_coalition

    from columns 
    where initiator_category = 'unit'
),


static as (
    select 
        hit_id,
        initiator_category,
        (initiator -> 'static' ->> 'id')::int as initiator_id,
        initiator -> 'static' ->> 'name' as initiator_name,
        initiator -> 'static' ->> 'type' as initiator_type,
        null::text as initiator_callsign,
        initiator -> 'static' ->> 'coalition' as initiator_coalition,
        (initiator -> 'static' -> 'position' -> 'u')::float as intiator_position_u,
        (initiator -> 'static' -> 'position' -> 'v')::float as intiator_position_v,
        (initiator -> 'static' -> 'position' -> 'lat')::float as initiator_latitude,
        (initiator -> 'static' -> 'position' -> 'lon')::float as initiator_longitude,
        (initiator -> 'static' -> 'position' -> 'alt')::float as initiator_altitude,
        (initiator -> 'static' -> 'velocity' -> 'speed')::float as initiator_speed,
        null::int as initiator_group_id,
        null::text as initiator_group_name,
        null::text as initiator_group_category,
        null::text as initiator_group_coalition

    from columns 
    where initiator_category = 'static'
),

scenery as (
    select 
        hit_id,
        initiator_category,
        (initiator -> 'scenery' ->> 'id')::int as initiator_id,
        null::text as initiator_name,
        initiator -> 'scenery' ->> 'type' as initiator_type,
        null::text as initiator_callsign,
        null::text as initiator_coalition,
        (initiator -> 'scenery' -> 'position' -> 'u')::float as intiator_position_u,
        (initiator -> 'scenery' -> 'position' -> 'v')::float as intiator_position_v,
        (initiator -> 'scenery' -> 'position' -> 'lat')::float as initiator_latitude,
        (initiator -> 'scenery' -> 'position' -> 'lon')::float as initiator_longitude,
        (initiator -> 'scenery' -> 'position' -> 'alt')::float as initiator_altitude,
        null::float as initiator_speed,
        null::int as initiator_group_id,
        null::text as initiator_group_name,
        null::text as initiator_group_category,
        null::text as initiator_group_coalition

    from columns 
    where initiator_category = 'scenery'
),

combined as (
    select * from units
    union all
    select * from static
    union all
    select * from scenery
    order by hit_id ASC
),


final as (
    select * from combined
)

select * from final
