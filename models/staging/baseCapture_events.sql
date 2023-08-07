SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'baseCapture' AS baseCapture,
        data->>'_baseCapture' AS _baseCapture
    FROM events
    WHERE data->>'baseCapture' IS NOT NULL