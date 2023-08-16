WITH
combined_events AS (
    SELECT * FROM {{ ref('combined_hits_kills') }}
),
eng_pairs AS (
    SELECT
        initiator_id,
        initiator_name,
        target_id,
        target_name,
        ROW_NUMBER() OVER (ORDER BY initiator_id, target_id) AS pair_id
    FROM combined_events
    GROUP BY initiator_id, initiator_name, target_id, target_name
    UNION
    SELECT
        target_id AS initiator_id,
        target_name AS initiator_name,
        initiator_id AS target_id,
        initiator_name AS target_name,
        ROW_NUMBER() OVER (ORDER BY target_id, initiator_id) + (SELECT COUNT(*) FROM combined_events) AS pair_id
    FROM combined_events
    GROUP BY target_id, target_name, initiator_id, initiator_name
),
engagement_events AS (
    SELECT
        combined_events.*,
        eng_pairs.pair_id AS pair_id
    FROM combined_events
    JOIN eng_pairs
    ON (
        combined_events.initiator_id = eng_pairs.initiator_id AND
        combined_events.target_id = eng_pairs.target_id
    ) OR (
        combined_events.initiator_id = eng_pairs.target_id AND
        combined_events.target_id = eng_pairs.initiator_id
    )
),
engagements AS (
    SELECT
        engagement_events.pair_id AS engagement_id,
        MIN(engagement_events.event_id) AS start_event_id,
        MIN(engagement_events.time_created) AS start_time,
        MAX(CASE WHEN event_type = 'kill' THEN engagement_events.event_id END) AS end_event_id,
        MAX(CASE WHEN event_type = 'kill' THEN engagement_events.time_created END) AS end_time,
        CASE
            WHEN MAX(event_type) = 'kill' THEN 'kill'
            WHEN MAX(event_type) = 'hit' THEN 'hit'
            ELSE 'miss'
        END AS outcome,
        COUNT(CASE WHEN event_type = 'shot' THEN 1 END) AS shots_count,
        COUNT(CASE WHEN event_type = 'hit' THEN 1 END) AS hits_count,
        MAX(CASE WHEN event_type = 'kill' THEN weapon_type END) AS kill_weapon,
        MAX(engagement_events.initiator_type) AS initiator_type,
        MAX(engagement_events.target_type) AS target_type,
        MAX(engagement_events.initiator_category) AS initiator_category,
        MAX(engagement_events.initiator_coalition) AS initiator_coalition,
        MAX(engagement_events.target_category) AS target_category,
        MAX(engagement_events.target_coalition) AS target_coalition
    FROM engagement_events
    GROUP BY engagement_events.pair_id
)
SELECT * FROM engagements