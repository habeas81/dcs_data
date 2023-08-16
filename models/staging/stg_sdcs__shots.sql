with

shot_events as (
    select * from {{ ref('base__shot_events')}}
),

shot_initiators as (
    select * from {{ ref('base__shot_initiators')}}
),

shot_weapons as (
    select * from {{ ref('base__shot_weapons')}}
),


shot_details as (
    select
        shot_id,
        campaign_id,
        initiator_user_id,
        time_created,
        weapon_name
    from shot_events
),


joined as (
    select
        -- ids
        details.shot_id,
        details.campaign_id,
        init.initiator_id,
        init.initiator_group_id,
        details.initiator_user_id,
        details.target_user_id,
        weapon.weapon_id,

        -- date/time
        details.time_created,

        -- text
        init.initiator_callsign,
        init.initiator_category,
        init.initiator_coalition,
        init.initiator_group_name,
        init.initiator_group_category,
        init.initiator_group_coalition,
        init.initiator_name,
        init.initiator_type,
        details.weapon_name,
        weapon.weapon_type,
        
        -- floats
        init.initiator_altitude,
        init.initiator_latitude,
        init.initiator_longitude,
        init.initiator_position_u,
        init.initiator_position_v,
        init.initiator_speed,
        weapon.weapon_altitude,
        weapon.weapon_latitude,
        weapon.weapon_longitude,
        weapon.weapon_heading,
        weapon.weapon_position_u,
        weapon.weapon_position_v,
        weapon.weapon_speed

    from shot_details as details
    left join shot_initiators as init
        on details.shot_id = init.shot_id
    left join shot_weapons as weapon
        on details.shot_id = weapon.shot_id
    order by details.shot_id ASC
),

cleaning as (
    select
        shot_id,
        campaign_id,
        initiator_id,
        initiator_group_id,
        {{target.schema}}.int_or_null(split_part(initiator_name, '|', 1)) as initiator_unit_id,
        initiator_user_id,
        weapon_id,

        -- date/time
        time_created,
        time_created::date as date_created,

        -- text
        {{- blanks_as_null('initiator_callsign') -}} as initiator_callsign,
        initiator_category,
        initcap(split_part(initiator_coalition, '_', 2)) as initiator_coalition,
        initiator_group_name,
        initcap(split_part(initiator_group_category, '_', 3)) as initiator_group_category,
        initcap(split_part(initiator_group_coalition, '_', 2)) as initiator_group_coalition,
        initiator_name,
        initiator_type,
        {{- blanks_as_null('target_callsign') -}} as target_callsign,
        weapon_name,
        weapon_type,
        
        -- floats
        initiator_altitude,
        initiator_latitude,
        initiator_longitude,
        initiator_position_u,
        initiator_position_v,
        initiator_speed,
        weapon_altitude,
        weapon_latitude,
        weapon_longitude,
        weapon_heading,
        weapon_position_u,
        weapon_position_v,
        weapon_speed
        from joined
),

final as (
    select * from cleaning
)

select * from final
