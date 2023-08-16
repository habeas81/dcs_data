{{
    config({
        "post-hook": [
            "{{ primary_key(this, 'unit_key') }}"
        ],
    })
}}

with

units as (
    select * from {{ ref('stg_sdcs__units') }}
),

unit_types as (
    select * from {{ ref('stg_sdcs__unit_types') }}
),


joined as (
    select distinct
        {{
            dbt_utils.generate_surrogate_key(
                [
                    'type.type_name',
                    'type.skill',
                    'type.unit_type',
                    'type.unit_class',
                    'unit.coalition',
                    'unit.player_can_drive',
                    'unit.unpacked_from_cargo',
                    'type.is_static',
                    'type.can_sling',
                    'type.can_drive',
                    'type.is_slot'
                ]
            )
        }} as unit_key,
        type.type_name,
        type.skill,
        type.unit_type,
        type.unit_class,
        unit.coalition,
        type.marker_category,
        unit.player_can_drive,
        unit.unpacked_from_cargo,
        type.is_static,
        type.can_sling,
        type.can_drive,
        type.is_slot

    from units as unit
    left join unit_types as type
        on unit.unit_type_id = type.id
),

final as (
    select * from joined
)

select * from final
