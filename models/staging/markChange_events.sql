SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'markChange' AS markChange,
        data->>'_markChange' AS _markChange
    FROM events
    WHERE data->>'markChange' IS NOT NULL