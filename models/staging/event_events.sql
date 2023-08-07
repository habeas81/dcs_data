SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'event' AS event,
        data->>'_event' AS _event
    FROM events
    WHERE data->>'event' IS NOT NULL