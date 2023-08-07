SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'markAdd' AS markAdd,
        data->>'_markAdd' AS _markAdd
    FROM events
    WHERE data->>'markAdd' IS NOT NULL