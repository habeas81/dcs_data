with

events as (
    select * from {{ ref('base_sdcs__events') }}
),

shots as (
    select * from events where events.data ->> 'event' = 'shootingStart'
),

top_level as (
    select
        shots.event_id,
        shots.campaign_id,
        shots.initiator_user_id,
        shots.target_user_id,
        shots.time_created,
        data."shootingStart" as shooting_start,
        data.time,
        data.event

    from shots,
    jsonb_to_record(data) as data("shootingStart" jsonb, time float, event text)
),

flatten as (
    select
        event_id,
        campaign_id,
        initiator_user_id,
        time_created,
        time,
        shot."weaponName" as weapon_name,
        initiator.heading as initiator_heading,
        initiator."numberInGroup" as initiator_number_in_group,
        initiator.initiator as initiator_category,
        unit.id as initiator_id,
        unit.fuel as initiator_fuel,
        unit.name as initiator_name,
        unit.type as initiator_type,
        unit."inAir" as initiator_started_in_air,
        unit.callsign as initiator_callsign,
        unit.coalition as initiator_coalition,
        unit."playerName" as initiator_player_name,
        group_.id as initiator_group_id,
        group_.name as initiator_group_name,
        group_.category as initiator_group_category,
        group_.coalition as initiator_group_coalition,
        position.u as initiator_position_u,
        position.v as initiator_position_v,
        position.alt as initiator_position_altitude,
        position.lat as initiator_position_latitude,
        position.lon as initiator_position_longitude,
        velocity.speed as initiator_velocity_speed,
        velocity.heading as initiator_velocity_heading,
        (velocity.velocity ->> 'x')::float as initiator_velocity_x,
        (velocity.velocity ->> 'y')::float as initiator_velocity_y,
        (velocity.velocity ->> 'z')::float as initiator_velocity_z,
        orientation.yaw as initiator_orientation_yaw,
        orientation.roll as initiator_orientation_roll,
        orientation.pitch as initiator_orientation_pitch,
        up.x as initiator_orientation_up_x,
        up.y as initiator_orientation_up_y,
        up.z as initiator_orientation_up_z,
        "right".x as initiator_orientation_right_x,
        "right".y as initiator_orientation_right_y,
        "right".z as initiator_orientation_right_z,
        forward.x as initiator_orientation_foward_x,
        forward.y as initiator_orientation_foward_y,
        forward.z as initiator_orientation_foward_z

    from top_level,
    jsonb_to_record(shooting_start) as shot("weaponName" text, initiator jsonb),
    jsonb_to_record(initiator) as initiator(unit jsonb, heading float, "numberInGroup" int, initiator text),
    jsonb_to_record(unit) as unit(
        id int,
        fuel float,
        name text,
        type text,
        "inAir" bool,
        callsign text,
        coalition text,
        "playerName" text,
        "group" jsonb,
        "position" jsonb,
        "velocity" jsonb,
        "orientation" jsonb
    ),
    jsonb_to_record("group") as group_(id int, name text, category text, coalition text),
    jsonb_to_record("position") as position(u float, v float, alt float, lat float, lon float),
    jsonb_to_record("velocity") as velocity(speed float, heading float, velocity jsonb),
    jsonb_to_record("orientation") as orientation(
        up jsonb,
        "right" jsonb,
        "forward" jsonb,
        yaw float,
        roll float,
        pitch float
    ),
    jsonb_to_record(up) as up(x float, y float, z float),
    jsonb_to_record("right") as "right"(x float, y float, z float),
    jsonb_to_record("forward") as forward(x float, y float, z float)
),

clean as (
    select

        --ids
        event_id,
        campaign_id,
        initiator_id,
        initiator_group_id,
        initiator_user_id,
        {{target.schema}}.int_or_null(split_part(initiator_name, '|', 1)) as initiator_unit_id,

        -- datetime
        time_created as date_time_created,
        time_created::date as date_created,
        time_created::time as time_created,
        time as dcs_time_created,

        --boolean
        coalesce(initiator_started_in_air, False) as initiator_started_in_air,

        --text
        {{ blanks_as_null('initiator_callsign') }} as initiator_callsign,
        initiator_category,
        initcap(
            coalesce(
                split_part(initiator_coalition, '_', 2),
                'NEUTRAL'
            )
        ) as initiator_coalition,
        initcap(
            split_part(initiator_group_category, '_', 3)
        ) as initiator_group_category,
        initcap(
            coalesce(
                split_part(initiator_group_coalition, '_', 2),
                'NEUTRAL'
            )
        ) as initiator_group_coalition,
        initiator_group_name,
        initiator_name,
        {{ blanks_as_null('initiator_player_name') }} as initiator_player_name,
        initiator_type,
        {{ blanks_as_null('weapon_name') }} as weapon_name,
        
        --float
        initiator_fuel,
        initiator_heading,
        initiator_position_altitude,
        initiator_position_latitude,
        initiator_position_longitude,
        initiator_position_u,
        initiator_position_v,
        initiator_number_in_group,
        initiator_velocity_heading,
        initiator_orientation_pitch,
        initiator_orientation_roll,
        initiator_orientation_yaw,
        initiator_orientation_foward_x,
        initiator_orientation_foward_y,
        initiator_orientation_foward_z,
        initiator_orientation_right_x,
        initiator_orientation_right_y,
        initiator_orientation_right_z,
        initiator_orientation_up_x,
        initiator_orientation_up_y,
        initiator_orientation_up_z,
        initiator_velocity_speed,
        initiator_velocity_x,
        initiator_velocity_y,
        initiator_velocity_z
    from flatten
),

final as (
    select * from clean
)

select * from final
