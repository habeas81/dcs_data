with

source as (
    select * from {{ ref('stg_sdcs__hits') }}
),

initiators as (
    select 
        initiator_callsign as callsign,
        initiator_category as category,
        initiator_coalition as coalition,
        initiator_group_name as group_name,
        initiator_group_category as group_category,
        initiator_group_coalition as group_coalition,
        initiator_name as name,
        initiator_type as type

    from source
),

targets as (
    select 
        target_callsign as callsign,
        target_category as category,
        target_coalition as coalition,
        target_group_name as group_name,
        target_group_category as group_category,
        target_group_coalition as group_coalition,
        target_name as name,
        target_type as type

    from source
),


combined as (
    select *
    from initiators
    union
    select *
    from targets
),

final as (
    select * from combined
)

select * from final
