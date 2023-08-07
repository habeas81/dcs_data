SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'unitLost' AS unitLost,
        data->>'_unitLost' AS _unitLost
    FROM events
    WHERE data->>'unitLost' IS NOT NULL