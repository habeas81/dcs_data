SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'engineStartup' AS engineStartup,
        data->>'_engineStartup' AS _engineStartup
    FROM events
    WHERE data->>'engineStartup' IS NOT NULL