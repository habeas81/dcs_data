with

source as (
    select * from {{source('sdcs', 'unit_type')}}
),

clean as (
    select
        -- ids
        id,
        unpack_type_id,

        -- text
        jtac_aggr_category as marker_category,
        shape_name,
        initcap(split_part(skill::text, '_', 2)) as skill,
        type_name,
        unit_type,
        initcap(unit_class::text) as unit_class,
        unpack_composition,

        -- boolean
        is_static,
        can_sling,
        can_drive,
        is_slot,

        -- int
        cargo_capacity,
        cargo_count,
        cargo_max_level,
        cargo_max_persist_level,
        factory_level,
        jtac_priority
        load_delay,
        mass_kg,
        shelter_level,
        troop_count,
        unit_level,
        unit_cost

    from source
),

final as (
    select * from clean
)

select * from final
