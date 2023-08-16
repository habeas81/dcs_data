with

source as (
    select * from {{ ref('base__hit_events')}}
),

clean as (
    select
        
        -- primary_id
        event_id,

        -- foreign_ids
        campaign_id,
        initiator_id::bigint as initiator_id,
        initiator_user_id,
        initiator_group_id::bigint as initiator_group_id,
        {{target.schema}}.int_or_null(split_part(initiator_name, '|', 1)) as initiator_unit_id,
        target_id::bigint as target_id,
        target_group_id::bigint as target_group_id,
        {{target.schema}}.int_or_null(split_part(target_name, '|', 1)) as target_unit_id,
        target_user_id,
        weapon_id::bigint as weapon_id,

        -- datetime
        time_created as datetime_created,
        time_created::date as date_created,
        time_created::time as time_created,

        -- boolean
        coalesce(initiator_in_air::boolean, False) as initiator_started_in_air,
        coalesce(target_in_air::boolean, False) as target_started_in_air,

        -- text
        initiator_category,
        {{ blanks_as_null('initiator_callsign') }} as initiator_callsign,
        initcap(
            coalesce(
                split_part(initiator_coalition, '_', 2),
                'NEUTRAL'
            )
        ) as initiator_coalition,
        initiator_name,
        initiator_type,
        initiator_group_name,
        initcap(
            split_part(initiator_group_category, '_', 3)
        ) as initiator_group_category,
        initcap(
            coalesce(
                split_part(initiator_group_coalition, '_', 2),
                'NEUTRAL'
            )
        ) as initiator_group_coalition,
        {{ blanks_as_null('initiator_player_name') }} as initiator_player_name,
        {{ blanks_as_null('target_callsign') }} as target_callsign,
        target_category,
        initcap(
            coalesce(
                split_part(target_coalition, '_', 2),
                'NEUTRAL'
            )
        ) as target_coalition,
        target_name,
        target_group_name,
        initcap(
            split_part(target_group_category, '_', 3)
        ) as target_group_category,
        initcap(
            coalesce(
                split_part(target_group_coalition, '_', 2),
                'NEUTRAL'
            )
        ) as target_group_coalition,
        {{ blanks_as_null('target_player_name') }} as target_player_name,
        target_type,
        {{ blanks_as_null('weapon_name') }} as weapon_name,
        weapon_type,

        -- integers
        initiator_number_in_group::int as initiator_number_in_group,
        target_number_in_group::int as target_number_in_group,

        -- float
        initiator_fuel::float as initiator_fuel,
        (initiator_position_u::float) as initiator_position_u,
        initiator_position_v::float as initiator_position_v,
        initiator_position_alt::float as initiator_position_altitude,
        initiator_position_lat::float as initiator_position_latitude,
        initiator_position_lon::float as initiator_position_longitude,
        initiator_velocity_speed::float as initiator_velocity_speed,
        initiator_velocity_heading::float as initiator_velocity_heading,
        initiator_velocity_x::float as initiator_velocity_x,
        initiator_velocity_y::float as initiator_velocity_y,
        initiator_orientation_up_x::float as initiator_orientation_up_x,
        initiator_orientation_up_y::float as initiator_orientation_up_y,
        initiator_orientation_up_z::float as initiator_orientation_up_z,
        initiator_orientation_yaw::float as initiator_orientation_yaw,
        initiator_orientation_roll::float as initiator_orientation_roll,
        initiator_orientation_pitch::float as initiator_orientation_pitch,
        initiator_orientation_right_x::float as initiator_orientation_right_x,
        initiator_orientation_right_y::float as initiator_orientation_right_y,
        initiator_orientation_right_z::float as initiator_orientation_right_z,
        initiator_orientation_forward_x::float as initiator_orientation_forward_x,
        initiator_orientation_forward_y::float as initiator_orientation_forward_y,
        initiator_orientation_forward_z::float as initiator_orientation_forward_z,
        initiator_orientation_heading::float as initiator_orientation_heading,
        target_fuel::float as target_fuel,
        target_position_u::float as target_position_u,
        target_position_v::float as target_position_v,
        target_position_alt::float as target_position_altitude,
        target_position_lat::float as target_position_latitude,
        target_position_lon::float as target_position_longitude,
        target_velocity_speed::float as target_velocity_speed,
        target_velocity_heading::float as target_velocity_heading,
        target_velocity_x::float as target_velocity_x,
        target_velocity_y::float as target_velocity_y,
        target_orientation_up_x::float as target_orientation_up_x,
        target_orientation_up_y::float as target_orientation_up_y,
        target_orientation_up_z::float as target_orientation_up_z,
        target_orientation_yaw::float as target_orientation_yaw,
        target_orientation_roll::float as target_orientation_roll,
        target_orientation_pitch::float as target_orientation_pitch,
        target_orientation_right_x::float as target_orientation_right_x,
        target_orientation_right_y::float as target_orientation_right_y,
        target_orientation_right_z::float as target_orientation_right_z,
        target_orientation_forward_x::float as target_orientation_forward_x,
        target_orientation_forward_y::float as target_orientation_forward_y,
        target_orientation_forward_z::float as target_orientation_forward_z,
        target_orientation_heading::float as target_orientation_heading,
        weapon_position_u::float as weapon_position_u,
        weapon_position_v::float as weapon_position_v,
        weapon_position_alt::float as weapon_position_altitude,
        weapon_position_lat::float as weapon_position_latitude,
        weapon_position_lon::float as weapon_position_longitude,
        weapon_velocity_speed::float as weapon_velocity_speed,
        weapon_velocity_heading::float as weapon_velocity_heading,
        weapon_velocity_velocity_x::float as weapon_velocity_velocity_x,
        weapon_velocity_velocity_y::float as weapon_velocity_velocity_y,
        weapon_velocity_velocity_z::float as weapon_velocity_velocity_z,
        weapon_orientation_up_x::float as weapon_orientation_up_x,
        weapon_orientation_up_y::float as weapon_orientation_up_y,
        weapon_orientation_up_z::float as weapon_orientation_up_z,
        weapon_orientation_yaw::float as weapon_orientation_yaw,
        weapon_orientation_roll::float as weapon_orientation_roll,
        weapon_orientation_pitch::float as weapon_orientation_pitch,
        weapon_orientation_right_x::float as weapon_orientation_right_x,
        weapon_orientation_right_y::float as weapon_orientation_right_y,
        weapon_orientation_right_z::float as weapon_orientation_right_z,
        weapon_orientation_forward_x::float as weapon_orientation_forward_x,
        weapon_orientation_forward_y::float as weapon_orientation_forward_y,
        weapon_orientation_forward_z::float as weapon_orientation_forward_z,
        weapon_orientation_heading::float as weapon_orientation_heading

        from source
),

final as (
    select * from clean
)

select * from final
