SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'takeoff' AS takeoff,
        data->>'_takeoff' AS _takeoff
    FROM events
    WHERE data->>'takeoff' IS NOT NULL