with

hit_events as (
    select * from {{ ref('base__hit_events')}}
),

hit_targets as (
    select * from {{ ref('base__hit_targets')}}
),

hit_initiators as (
    select * from {{ ref('base__hit_initiators')}}
),

hit_weapons as (
    select * from {{ ref('base__hit_weapons')}}
),


hit_details as (
    select
        hit_id,
        campaign_id,
        initiator_user_id,
        target_user_id,
        time_created,
        weapon_name
    from hit_events
),


joined as (
    select
        -- ids
        details.hit_id,
        details.campaign_id,
        init.initiator_id,
        init.initiator_group_id,
        details.initiator_user_id,
        target.target_id,
        details.target_user_id,
        target.target_group_id,
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
        target.target_callsign,
        target.target_category,
        target.target_coalition,
        target.target_group_category,
        target.target_group_coalition,
        target.target_group_name,
        target.target_name,
        target.target_type,
        details.weapon_name,
        weapon.weapon_type,
        
        -- floats
        init.initiator_altitude,
        init.initiator_latitude,
        init.initiator_longitude,
        init.initiator_position_u,
        init.initiator_position_v,
        init.initiator_speed,
        target.target_altitude,
        target.target_latitude,
        target.target_longitude,
        target.target_speed,
        weapon.weapon_altitude,
        weapon.weapon_latitude,
        weapon.weapon_longitude,
        weapon.weapon_heading,
        weapon.weapon_position_u,
        weapon.weapon_position_v,
        weapon.weapon_speed

    from hit_details as details
    left join hit_targets as target
        on details.hit_id = target.hit_id
    left join hit_initiators as init
        on details.hit_id = init.hit_id
    left join hit_weapons as weapon
        on details.hit_id = weapon.hit_id
    order by details.hit_id ASC
),

cleaning as (
    select
        hit_id,
        campaign_id,
        initiator_id,
        initiator_group_id,
        initiator_user_id,
        target_id,
        target_user_id,
        target_group_id,
        weapon_id,

        -- date/time
        time_created,
        time_created::date as date_created,

        -- text
        {{- blanks_as_null('initiator_callsign') -}} as initiator_callsign,
        initiator_category,
        split_part(initiator_coalition, '_', 2) as initiator_coalition,
        initiator_group_name,
        split_part(initiator_group_category, '_', 3) as initiator_group_category,
        split_part(initiator_group_coalition, '_', 2) as initiator_group_coalition,
        initiator_name,
        initiator_type,
        {{- blanks_as_null('target_callsign') -}} as target_callsign,
        target_category,
        split_part(target_coalition, '_', 2) as target_coalition,
        split_part(target_group_category, '_', 3) as target_group_category,
        split_part(target_group_coalition, '_', 2) as target_group_coalition,
        target_group_name,
        target_name,
        target_type,
        weapon_name,
        weapon_type,
        
        -- floats
        initiator_altitude,
        initiator_latitude,
        initiator_longitude,
        initiator_position_u,
        initiator_position_v,
        initiator_speed,
        target_altitude,
        target_latitude,
        target_longitude,
        target_speed,
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
