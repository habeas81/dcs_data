WITH combined_events_initiator AS (
    SELECT
        'shot' AS event_type,
        s.initiator_user_id,
        COALESCE(s.initiator_type, 'unknown') AS initiator_type,
        COALESCE(s.target_type, 'unknown') AS target_type,
        s.campaign_id
    FROM {{ ref('stg_sdcs__shots') }} s

    UNION ALL

    SELECT
        'hit' AS event_type,
        h.initiator_user_id,
        COALESCE(h.initiator_type, 'unknown') AS initiator_type,
        COALESCE(h.target_type, 'unknown') AS target_type,
        h.campaign_id
    FROM {{ ref('stg_sdcs__hits') }} h

    UNION ALL

    SELECT
        'kill' AS event_type,
        k.initiator_user_id,
        COALESCE(k.initiator_type, 'unknown') AS initiator_type,
        COALESCE(k.target_type, 'unknown') AS target_type,
        k.campaign_id
    FROM {{ ref('stg_sdcs__kills') }} k
),
combined_events_target AS (
    SELECT
        'shot' AS event_type,
        s.target_user_id,
        COALESCE(s.initiator_type, 'unknown') AS initiator_type,
        COALESCE(s.target_type, 'unknown') AS target_type,
        s.campaign_id
    FROM {{ ref('stg_sdcs__shots') }} s

    UNION ALL

    SELECT
        'hit' AS event_type,
        h.target_user_id,
        COALESCE(h.initiator_type, 'unknown') AS initiator_type,
        COALESCE(h.target_type, 'unknown') AS target_type,
        h.campaign_id
    FROM {{ ref('stg_sdcs__hits') }} h

    UNION ALL

    SELECT
        'kill' AS event_type,
        k.target_user_id,
        COALESCE(k.initiator_type, 'unknown') AS initiator_type,
        COALESCE(k.target_type, 'unknown') AS target_type,
        k.campaign_id
    FROM {{ ref('stg_sdcs__kills') }} k
),
initiator_stats AS (
    SELECT
        u.id AS user_id,
        u.name AS user_name,
        ce.campaign_id,
        ce.initiator_type,
        ce.target_type,
        COUNT(CASE WHEN ce.event_type = 'shot' THEN 1 END) AS shots_count,
        COUNT(CASE WHEN ce.event_type = 'hit' THEN 1 END) AS hits_count,
        COUNT(CASE WHEN ce.event_type = 'kill' THEN 1 END) AS kills_count
    FROM
        sdcs_data.public.user u
        JOIN combined_events_initiator ce ON u.id = ce.initiator_user_id
    GROUP BY
        u.id,
        u.name,
        ce.campaign_id,
        ce.initiator_type,
        ce.target_type
),
target_stats AS (
    SELECT
        u.id AS user_id,
        u.name AS user_name,
        ce.campaign_id,
        ce.initiator_type,
        ce.target_type,
        COUNT(CASE WHEN ce.event_type = 'shot' THEN 1 END) AS shot_at_count,
        COUNT(CASE WHEN ce.event_type = 'hit' THEN 1 END) AS hit_count,
        COUNT(CASE WHEN ce.event_type = 'kill' THEN 1 END) AS killed_count
    FROM
        sdcs_data.public.user u
        JOIN combined_events_target ce ON u.id = ce.target_user_id
    GROUP BY
        u.id,
        u.name,
        ce.campaign_id,
        ce.initiator_type,
        ce.target_type
)
SELECT
    i.user_id,
    i.user_name,
    i.campaign_id,
    i.initiator_type,
    i.target_type,
    i.shots_count,
    i.hits_count,
    i.kills_count,
    t.shot_at_count,
    t.hit_count,
    t.killed_count
FROM initiator_stats i
LEFT JOIN target_stats t
ON i.user_id = t.user_id
AND i.campaign_id = t.campaign_id
AND i.initiator_type = t.initiator_type
AND i.target_type = t.target_type
ORDER BY i.user_id
