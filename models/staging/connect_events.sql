SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'connect' AS connect,
        data->>'_connect' AS _connect
    FROM events
    WHERE data->>'connect' IS NOT NULL