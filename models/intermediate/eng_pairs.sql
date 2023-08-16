
WITH eng_pairs AS (
    SELECT
        initiator_id,
        initiator_name,
        target_id,
        target_name,
        ROW_NUMBER() OVER (ORDER BY initiator_id, target_id) AS pair_id
    FROM (
        

WITH shots_combined AS (
    SELECT
        event_id,
        initiator_id AS initiator_id,
        initiator_name AS initiator_name,
        target_id AS target_id,
        target_name AS target_name,
        'shot' AS event_type,
        time_created::time AS event_time,
        weapon_type AS weapon_type
    FROM {{ ref('stg_sdcs__shots') }}
)


, hits_combined AS (
    SELECT
        event_id,
        initiator_id AS initiator_id,
        initiator_name AS initiator_name,
        target_id AS target_id,
        target_name AS target_name,
        'hit' AS event_type,
        time_created::time AS event_time,
        weapon_type AS weapon_type
    FROM {{ ref('stg_sdcs__hits') }}
)


, kills_combined AS (
    SELECT
        event_id,
        initiator_id AS initiator_id,
        initiator_name AS initiator_name,
        target_id AS target_id,
        target_name AS target_name,
        'kill' AS event_type,
        time_created::time AS event_time,
        weapon_type AS weapon_type
    FROM {{ ref('stg_sdcs__kills') }}
)

SELECT * FROM shots_combined
UNION ALL
SELECT * FROM hits_combined
UNION ALL
SELECT * FROM kills_combined

    ) combined_events
    GROUP BY initiator_id, initiator_name, target_id, target_name
)
SELECT * FROM eng_pairs
