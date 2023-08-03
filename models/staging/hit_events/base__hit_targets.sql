with

hits as (
    select * from {{ ref('base__hit_events') }}
),

columns as (
    select
        hit_id,
        target ->> 'target' as target_category,
        target
    from hits
),

units as (
    select 
        hit_id,
        target_category,
        (target -> 'unit' ->> 'id')::int as target_id,
        target -> 'unit' ->> 'name' as target_name,
        target -> 'unit' ->> 'type' as target_type,
        target -> 'unit' ->> 'callsign' as target_callsign,
        target -> 'unit' ->> 'coalition' as target_coalition,
        (target -> 'unit' -> 'position' ->> 'lat')::float as target_latitude,
        (target -> 'unit' -> 'position' ->> 'lon')::float as target_longitude,
        (target -> 'unit' -> 'position' ->> 'alt')::float as target_altitude,
        (target -> 'unit' -> 'velocity' ->> 'speed')::float as target_speed,
        (target -> 'unit' -> 'group' ->> 'id')::int as target_group_id,
        target -> 'unit' -> 'group' ->> 'name' as target_group_name,
        target -> 'unit' -> 'group' ->> 'category' as target_group_category,
        target -> 'unit' -> 'group' ->> 'coalition' as target_group_coalition

    from columns 
    where target_category = 'unit'
),

scenery as (
    select 
        hit_id,
        target_category,
        (target -> 'scenery' ->> 'id')::int as target_id,
        null::text as target_name,
        target -> 'scenery' ->> 'type' as target_type,
        null::text as target_callsign,
        null::text as target_coalition,
        (target -> 'scenery' -> 'position' ->> 'lat')::float as target_latitude,
        (target -> 'scenery' -> 'position' ->> 'lon')::float as target_longitude,
        (target -> 'scenery' -> 'position' ->> 'alt')::float as target_altitude,
        null::float as target_speed,
        null::int as target_group_id,
        null::text as target_group_name,
        null::text as target_group_category,
        null::text as target_group_coalition

    from columns 
    where target_category = 'scenery'
),

static as (
    select 
        hit_id,
        target_category,
        (target -> 'static' ->> 'id')::int as target_id,
        target -> 'static' ->> 'name' as target_name,
        target -> 'static' ->> 'type' as target_type,
        null::text as target_callsign,
        target -> 'static' ->> 'coalition' as target_coalition,
        (target -> 'static' -> 'position' ->> 'lat')::float as target_latitude,
        (target -> 'static' -> 'position' ->> 'lon')::float as target_longitude,
        (target -> 'static' -> 'position' ->> 'alt')::float as target_altitude,
        (target -> 'static' -> 'velocity' ->> 'speed')::float as target_speed,
        null::int as target_group_id,
        null::text as target_group_name,
        null::text as target_group_category,
        null::text as target_group_coalition

    from columns 
    where target_category = 'static'
),

combined as (
    select * 
    from units
    union all
    select * 
    from scenery
    union all
    select *
    from static
),

final as (
    select * from combined
    order by hit_id ASC
)

select * from final
