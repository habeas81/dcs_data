SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'land' AS land,
        data->>'_land' AS _land
    FROM events
    WHERE data->>'land' IS NOT NULL