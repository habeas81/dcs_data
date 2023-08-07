SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'kill' AS kill,
        data->>'_kill' AS _kill
    FROM events
    WHERE data->>'kill' IS NOT NULL