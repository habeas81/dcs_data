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
        target -> 'unit' ->> 'name' as target_name,
        target -> 'unit' ->> 'type' as target_type,
        target -> 'unit' ->> 'callsign' as target_callsign,
        target -> 'unit' ->> 'coalition' as target_coalition,
        (target -> 'unit' -> 'position' ->> 'lat')::float as target_latitude,
        (target -> 'unit' -> 'position' ->> 'lon')::float as target_longitude,
        (target -> 'unit' -> 'position' ->> 'alt')::float as target_altitude,
        (target -> 'unit' -> 'velocity' ->> 'speed')::float as target_speed,
        target -> 'unit' -> 'group' ->> 'id' as target_group_id,
        target -> 'unit' -> 'group' ->> 'name' as target_group_name,
        target -> 'unit' -> 'group' ->> 'category' as target_group_category,
        target -> 'unit' -> 'group' ->> 'coalition' as target_group_coalition


    from columns 
    where target_category = 'unit'
),

final as (
    select * from units
)

select * from final
