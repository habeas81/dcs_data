with

source as (
    select * from {{ source('sdcs', 'campaign') }}
),

map_names as (
    select * from {{ ref('seed__maps') }}
),

columns as (
    select
        -- ids
        id as campaign_id,

        -- datetime
        start as datetime_started,
        "end" as datetime_ended,

        -- date
        start::date as date_started,
        "end"::date as date_ended,

        -- time
        start::time as time_started,
        "end"::time as time_ended,

        -- integers
        ("end"::date - start::date)::int as campaign_length,

        -- text
        source.theatre || '-' || rotation as name,
        source.theatre::text,
        map_names.map,
        rotation,
        initcap(
            coalesce(
                winner::text,
                'Not Ended'
            )
        ) as winning_coalition,

        -- enums
        source.theatre as theatre_enum,
        winner as winner_enum

    from source
    left join map_names
        on map_names.theatre = source.theatre::text
),

final as (
    select * from columns
)

select * from final
