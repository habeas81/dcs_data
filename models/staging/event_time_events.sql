SELECT id, campaign_id, initiator_user_id, target_user_id, time, data->>'time' AS event_time
FROM {{ ref('events') }} WHERE data->>'time' IS NOT NULL
