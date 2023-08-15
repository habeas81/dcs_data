

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
)


, kills_combined AS (
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
)

SELECT * FROM hits_combined
UNION ALL
SELECT * FROM kills_combined
