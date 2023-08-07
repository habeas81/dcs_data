SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'refueling' AS refueling,
        data->>'_refueling' AS _refueling
    FROM events
    WHERE data->>'refueling' IS NOT NULL