with

source as (
    select * from {{source('sdcs', 'unit')}}
),

retype as (
    select
        --ids
        id,
        campaign_id,
        group_id,
        spawn_airfield_id,
        user_id,
        unit_id,
        unit_type_id,

        --datetime
        created_at as datetime_created,
        created_at::date as date_created,
        created_at::time as time_created,
        removed_at as datetime_removed,
        removed_at::date as date_removed,
        removed_at::time as time_removed,
        zone_entered_at as datetime_entered_zone,
        zone_entered_at::date as date_entered_zone,
        zone_entered_at::time as time_entered_zone,

        --boolean
        player_can_drive,
        respawned,
        respawn_when_killed,
        unpacked_from_cargo,

        --text
        initcap(coalition::text) as coalition,
        unit_suffix,
        spawn_zone,
        removed_reason,
        replace_with_comp,
        zone_entered,

        --int
        mass_kg,
        spawn_location_level,
        removed_at_prio as removed_at_priority,

        --float
        hdg as heading,
        pos_u as position_u,
        pos_v as position_v,
        spawn_pos_u as spawn_position_u,
        spawn_pos_v as spawn_position_v

    from source
),

final as (
    select * from retype
)

select * from final
