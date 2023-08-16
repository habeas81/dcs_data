{{
    config(
        materialized="ephemeral"
    )
}}

with

events as (
    select * from {{ ref('base_sdcs__events') }}
),

kills as (
    select * from events where events.data ->> 'event' = 'kill'
),

top_level as (
    select
        kills.event_id,
        kills.campaign_id,
        kills.initiator_user_id,
        kills.target_user_id,
        kills.time_created,
        data.kill ->> 'weaponName' as weapon_name,
        data.kill,
        data.time,
        data.event

    from kills,
    jsonb_to_record(data) as data(kill jsonb, time float, event text)
),

targets_raw as (
    select
        event.event_id,
        kill.target as target

    from top_level as event,
    jsonb_to_record(event.kill) as kill(target jsonb)
),

weapons_raw as (
    select
        event.event_id,
        kill.weapon as weapon

    from top_level as event,
    jsonb_to_record(event.kill) as kill(weapon jsonb)
),

initiator_raw as (
    select
        event.event_id,
        kill.initiator as initiator

    from top_level as event,
    jsonb_to_record(event.kill) as kill(initiator jsonb)
),

unit_targets as (
    select
        target.event_id,
        {{- flatten_unit("target -> 'unit'", "target_") -}},
        target ->> 'target' as target_category

    from targets_raw as target
    where target ->> 'target' = 'unit'
),

scenery_targets as (
    select
        target.event_id,
        {{- flatten_scenery("target -> 'scenery'", "target_")}},
        target ->> 'target' as target_category

    from targets_raw as target
    where target ->> 'target' = 'scenery'
),

static_targets as (
    select
    target.event_id,
    {{- flatten_static("target -> 'static'", 'target_') -}},
    target ->> 'target' as target_category

    from targets_raw as target
    where target ->> 'target' = 'static'
),

targets as (
    select 
        event_id,
        target_id,
        target_name,
        target_type,
        null as target_fuel,
        null as target_group_id,
        null as target_group_name,
        null as target_group_category,
        null as target_group_coalition,
        null as target_in_air,
        null as target_callsign,
        target_position_u,
        target_position_v,
        target_position_alt,
        target_position_lat,
        target_position_lon,
        target_velocity_speed,
        target_velocity_heading,
        target_velocity_x,
        target_velocity_y,
        null as target_player_name,
        target_coalition,
        target_orientation_up_x,
        target_orientation_up_y,
        target_orientation_up_z,
        target_orientation_yaw,
        target_orientation_roll,
        target_orientation_pitch,
        target_orientation_right_x,
        target_orientation_right_y,
        target_orientation_right_z,
        target_orientation_forward_x,
        target_orientation_forward_y,
        target_orientation_forward_z,
        target_orientation_heading,
        null as target_number_in_group,
        target_category
    from static_targets
    union all
    select
        event_id,
        target_id,
        target_name,
        target_type,
        target_fuel,
        target_group_id,
        target_group_name,
        target_group_category,
        target_group_coalition,
        target_in_air,
        target_callsign,
        target_position_u,
        target_position_v,
        target_position_alt,
        target_position_lat,
        target_position_lon,
        target_velocity_speed,
        target_velocity_heading,
        target_velocity_x,
        target_velocity_y,
        target_player_name,
        target_coalition,
        target_orientation_up_x,
        target_orientation_up_y,
        target_orientation_up_z,
        target_orientation_yaw,
        target_orientation_roll,
        target_orientation_pitch,
        target_orientation_right_x,
        target_orientation_right_y,
        target_orientation_right_z,
        target_orientation_forward_x,
        target_orientation_forward_y,
        target_orientation_forward_z,
        target_orientation_heading,
        target_number_in_group,
        target_category
    from unit_targets
    union all
    select 
        event_id,
        target_id,
        null as target_name,
        target_type,
        null as target_fuel,
        null as target_group_id,
        null as target_group_name,
        null as target_group_category,
        null as target_group_coalition,
        null as target_in_air,
        null as target_callsign,
        target_position_u,
        target_position_v,
        target_position_alt,
        target_position_lat,
        target_position_lon,
        null as target_velocity_speed,
        null as target_velocity_heading,
        null as target_velocity_x,
        null as target_velocity_y,
        null as target_player_name,
        null as target_coalition,
        null as target_orientation_up_x,
        null as target_orientation_up_y,
        null as target_orientation_up_z,
        null as target_orientation_yaw,
        null as target_orientation_roll,
        null as target_orientation_pitch,
        null as target_orientation_right_x,
        null as target_orientation_right_y,
        null as target_orientation_right_z,
        null as target_orientation_forward_x,
        null as target_orientation_forward_y,
        null as target_orientation_forward_z,
        null as target_orientation_heading,
        null as target_number_in_group,
        target_category
    from scenery_targets
),

weapons as (
    select
        event_id,
        weapon ->> 'id' as weapon_id,
        weapon ->> 'type' as weapon_type,
        weapon -> 'position' ->> 'u' as weapon_position_u,
        weapon -> 'position' ->> 'v' as weapon_position_v,
        weapon -> 'position' ->> 'alt' as weapon_position_alt,
        weapon -> 'position' ->> 'lat' as weapon_position_lat,
        weapon -> 'position' ->> 'lon' as weapon_position_lon,
        weapon -> 'velocity' ->> 'speed' as weapon_velocity_speed,
        weapon -> 'velocity' ->> 'heading' as weapon_velocity_heading,
        weapon -> 'velocity' -> 'velocity' ->> 'x' as weapon_velocity_velocity_x,
        weapon -> 'velocity' -> 'velocity' ->> 'y' as weapon_velocity_velocity_y,
        weapon -> 'velocity' -> 'velocity' ->> 'z' as weapon_velocity_velocity_z,
        weapon -> 'orientation' -> 'up' ->> 'x' as weapon_orientation_up_x,
        weapon -> 'orientation' -> 'up' ->> 'y' as weapon_orientation_up_y,
        weapon -> 'orientation' -> 'up' ->> 'z' as weapon_orientation_up_z,
        weapon -> 'orientation' ->> 'yaw' as weapon_orientation_yaw,
        weapon -> 'orientation' ->> 'roll' as weapon_orientation_roll,
        weapon -> 'orientation' ->> 'pitch' as weapon_orientation_pitch,
        weapon -> 'orientation' -> 'right' ->> 'x' as weapon_orientation_right_x,
        weapon -> 'orientation' -> 'right' ->> 'y' as weapon_orientation_right_y,
        weapon -> 'orientation' -> 'right' ->> 'z' as weapon_orientation_right_z,
        weapon -> 'orientation' -> 'forward' ->> 'x' as weapon_orientation_forward_x,
        weapon -> 'orientation' -> 'forward' ->> 'y' as weapon_orientation_forward_y,
        weapon -> 'orientation' -> 'forward' ->> 'z' as weapon_orientation_forward_z,
        weapon -> 'orientation' ->> 'heading' as weapon_orientation_heading
    from weapons_raw as w 
    where weapon -> 'id' is not null
),

unit_initiator as (
    select
        initiator.event_id,
        {{- flatten_unit("initiator -> 'unit'", "initiator_") -}},
        initiator ->> 'target' as initiator_category

    from initiator_raw as initiator
    where initiator ->> 'initiator' = 'unit'
),

scenery_initiator as (
    select
        initiator.event_id,
        {{- flatten_scenery("initiator -> 'scenery'", "initiator_") -}},
        initiator ->> 'target' as initiator_category

    from initiator_raw as initiator
    where initiator ->> 'initiator' = 'scenery'
),

static_initiators as (
    select
        initiator.event_id,
        {{- flatten_static("initiator -> 'static'", "initiator_")}},
        initiator ->> 'target' as initiator_category

    from initiator_raw as initiator
    where initiator ->> 'initiator' = 'static'
),

initiators as (
    select 
        event_id,
        initiator_id,
        initiator_name,
        initiator_type,
        null as initiator_fuel,
        null as initiator_group_id,
        null as initiator_group_name,
        null as initiator_group_category,
        null as initiator_group_coalition,
        null as initiator_in_air,
        null as initiator_callsign,
        initiator_position_u,
        initiator_position_v,
        initiator_position_alt,
        initiator_position_lat,
        initiator_position_lon,
        initiator_velocity_speed,
        initiator_velocity_heading,
        initiator_velocity_x,
        initiator_velocity_y,
        null as initiator_player_name,
        initiator_coalition,
        initiator_orientation_up_x,
        initiator_orientation_up_y,
        initiator_orientation_up_z,
        initiator_orientation_yaw,
        initiator_orientation_roll,
        initiator_orientation_pitch,
        initiator_orientation_right_x,
        initiator_orientation_right_y,
        initiator_orientation_right_z,
        initiator_orientation_forward_x,
        initiator_orientation_forward_y,
        initiator_orientation_forward_z,
        initiator_orientation_heading,
        null as initiator_number_in_group,
        initiator_category
    from static_initiators
    union all
    select
        event_id,
        initiator_id,
        initiator_name,
        initiator_type,
        initiator_fuel,
        initiator_group_id,
        initiator_group_name,
        initiator_group_category,
        initiator_group_coalition,
        initiator_in_air,
        initiator_callsign,
        initiator_position_u,
        initiator_position_v,
        initiator_position_alt,
        initiator_position_lat,
        initiator_position_lon,
        initiator_velocity_speed,
        initiator_velocity_heading,
        initiator_velocity_x,
        initiator_velocity_y,
        initiator_player_name,
        initiator_coalition,
        initiator_orientation_up_x,
        initiator_orientation_up_y,
        initiator_orientation_up_z,
        initiator_orientation_yaw,
        initiator_orientation_roll,
        initiator_orientation_pitch,
        initiator_orientation_right_x,
        initiator_orientation_right_y,
        initiator_orientation_right_z,
        initiator_orientation_forward_x,
        initiator_orientation_forward_y,
        initiator_orientation_forward_z,
        initiator_orientation_heading,
        initiator_number_in_group,
        initiator_category
    from unit_initiator
    union all
    select 
        event_id,
        initiator_id,
        null as initiator_name,
        initiator_type,
        null as initiator_fuel,
        null as initiator_group_id,
        null as initiator_group_name,
        null as initiator_group_category,
        null as initiator_group_coalition,
        null as initiator_in_air,
        null as initiator_callsign,
        initiator_position_u,
        initiator_position_v,
        initiator_position_alt,
        initiator_position_lat,
        initiator_position_lon,
        null as initiator_velocity_speed,
        null as initiator_velocity_heading,
        null as initiator_velocity_x,
        null as initiator_velocity_y,
        null as initiator_player_name,
        null as initiator_coalition,
        null as initiator_orientation_up_x,
        null as initiator_orientation_up_y,
        null as initiator_orientation_up_z,
        null as initiator_orientation_yaw,
        null as initiator_orientation_roll,
        null as initiator_orientation_pitch,
        null as initiator_orientation_right_x,
        null as initiator_orientation_right_y,
        null as initiator_orientation_right_z,
        null as initiator_orientation_forward_x,
        null as initiator_orientation_forward_y,
        null as initiator_orientation_forward_z,
        null as initiator_orientation_heading,
        null as initiator_number_in_group,
        initiator_category
    from scenery_initiator
),

rejoined as (
    select
        top_level.event_id,
        top_level.campaign_id,
        top_level.initiator_user_id,
        top_level.target_user_id,
        top_level.time_created,
        top_level.weapon_name,
        initiator.initiator_id,
        initiator.initiator_name,
        initiator.initiator_type,
        initiator.initiator_fuel,
        initiator.initiator_group_id,
        initiator.initiator_group_name,
        initiator.initiator_group_category,
        initiator.initiator_group_coalition,
        initiator.initiator_in_air,
        initiator.initiator_callsign,
        initiator.initiator_position_u,
        initiator.initiator_position_v,
        initiator.initiator_position_alt,
        initiator.initiator_position_lat,
        initiator.initiator_position_lon,
        initiator.initiator_velocity_speed,
        initiator.initiator_velocity_heading,
        initiator.initiator_velocity_x,
        initiator.initiator_velocity_y,
        initiator.initiator_player_name,
        initiator.initiator_coalition,
        initiator.initiator_orientation_up_x,
        initiator.initiator_orientation_up_y,
        initiator.initiator_orientation_up_z,
        initiator.initiator_orientation_yaw,
        initiator.initiator_orientation_roll,
        initiator.initiator_orientation_pitch,
        initiator.initiator_orientation_right_x,
        initiator.initiator_orientation_right_y,
        initiator.initiator_orientation_right_z,
        initiator.initiator_orientation_forward_x,
        initiator.initiator_orientation_forward_y,
        initiator.initiator_orientation_forward_z,
        initiator.initiator_orientation_heading,
        initiator.initiator_number_in_group,
        initiator.initiator_category,
        targets.target_id,
        targets.target_name,
        targets.target_type,
        targets.target_fuel,
        targets.target_group_id,
        targets.target_group_name,
        targets.target_group_category,
        targets.target_group_coalition,
        targets.target_in_air,
        targets.target_callsign,
        targets.target_position_u,
        targets.target_position_v,
        targets.target_position_alt,
        targets.target_position_lat,
        targets.target_position_lon,
        targets.target_velocity_speed,
        targets.target_velocity_heading,
        targets.target_velocity_x,
        targets.target_velocity_y,
        targets.target_player_name,
        targets.target_coalition,
        targets.target_orientation_up_x,
        targets.target_orientation_up_y,
        targets.target_orientation_up_z,
        targets.target_orientation_yaw,
        targets.target_orientation_roll,
        targets.target_orientation_pitch,
        targets.target_orientation_right_x,
        targets.target_orientation_right_y,
        targets.target_orientation_right_z,
        targets.target_orientation_forward_x,
        targets.target_orientation_forward_y,
        targets.target_orientation_forward_z,
        targets.target_orientation_heading,
        targets.target_number_in_group,
        targets.target_category,
        weapon.weapon_id,
        weapon.weapon_type,
        weapon.weapon_position_u,
        weapon.weapon_position_v,
        weapon.weapon_position_alt,
        weapon.weapon_position_lat,
        weapon.weapon_position_lon,
        weapon.weapon_velocity_speed,
        weapon.weapon_velocity_heading,
        weapon.weapon_velocity_velocity_x,
        weapon.weapon_velocity_velocity_y,
        weapon.weapon_velocity_velocity_z,
        weapon.weapon_orientation_up_x,
        weapon.weapon_orientation_up_y,
        weapon.weapon_orientation_up_z,
        weapon.weapon_orientation_yaw,
        weapon.weapon_orientation_roll,
        weapon.weapon_orientation_pitch,
        weapon.weapon_orientation_right_x,
        weapon.weapon_orientation_right_y,
        weapon.weapon_orientation_right_z,
        weapon.weapon_orientation_forward_x,
        weapon.weapon_orientation_forward_y,
        weapon.weapon_orientation_forward_z,
        weapon.weapon_orientation_heading
    from top_level
    left join targets on targets.event_id = top_level.event_id
    left join weapons as weapon on weapon.event_id = top_level.event_id
    left join initiators as initiator on initiator.event_id = top_level.event_id
),


final as (
    select * from rejoined
)

select * from final
