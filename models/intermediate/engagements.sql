
WITH hits_combined AS (
    SELECT
        hit_id AS event_id,
        initiator_id::text AS initiator_id,
        initiator_name AS initiator_name,
        target_id::text AS target_id,
        target_name AS target_name,
        'hit' AS event_type,
        time_created::time AS event_time,
        weapon_type AS weapon_type
    FROM {{ ref('stg_sdcs__hits') }}
),
kills_combined AS (
    SELECT
        event_id AS event_id,
        initiator_id::text AS initiator_id,
        initiator_name AS initiator_name,
        target_id::text AS target_id,
        target_name AS target_name,
        'kill' AS event_type,
        time_created::time AS event_time,
        weapon_type AS weapon_type
    FROM {{ ref('stg_sdcs__kills') }}
),
combined_events_alias AS (
    SELECT * FROM hits_combined
    UNION ALL
    SELECT * FROM kills_combined
),
engagement_events AS (
    SELECT
        combined_events_alias.*,
        eng_pairs.pair_id::text AS pair_id
    FROM combined_events_alias
    JOIN {{ ref('eng_pairs') }}
    ON combined_events_alias.initiator_id = eng_pairs.initiator_id
    AND combined_events_alias.target_id = eng_pairs.target_id
)
, engagements AS (
    SELECT
        engagement_events.pair_id::text AS engagement_id, -- This alias
        MIN(event_id) AS start_event_id,
        MAX(event_id) AS end_event_id,
        engagement_events.pair_id::text AS pair_id -- Include this line
    FROM engagement_events
    GROUP BY engagement_events.pair_id
)
SELECT * FROM engagements
