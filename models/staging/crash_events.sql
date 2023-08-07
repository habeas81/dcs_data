SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'crash' AS crash,
        data->>'_crash' AS _crash
    FROM events
    WHERE data->>'crash' IS NOT NULL