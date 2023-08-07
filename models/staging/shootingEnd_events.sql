SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'shootingEnd' AS shootingEnd,
        data->>'_shootingEnd' AS _shootingEnd
    FROM events
    WHERE data->>'shootingEnd' IS NOT NULL