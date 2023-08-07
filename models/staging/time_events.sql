SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'time' AS time,
        data->>'_time' AS _time
    FROM events
    WHERE data->>'time' IS NOT NULL