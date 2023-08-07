SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'disconnect' AS disconnect,
        data->>'_disconnect' AS _disconnect
    FROM events
    WHERE data->>'disconnect' IS NOT NULL