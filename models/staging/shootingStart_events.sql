SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'shootingStart' AS shootingStart,
        data->>'_shootingStart' AS _shootingStart
    FROM events
    WHERE data->>'shootingStart' IS NOT NULL