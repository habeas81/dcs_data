with

source as (
    select * from {{ source('sdcs', 'airbase')}}
),

reorder as (
    select
        -- ids
        id,
        airbase_id,
        campaign_id,

        -- strings
        {{- fill_na('coalition::TEXT', 'NEUTRAL') -}} as coalition,
        name as airbase_name,

        -- booleans
        damaged as is_damaged,
        functional as is_functional,

        --integers
        level,

        --floats
        pos_u as dcs_coordinates_u,
        pos_v as dcs_coordinates_v

    from source
),

final as (
    select * from reorder
)

select * from final
