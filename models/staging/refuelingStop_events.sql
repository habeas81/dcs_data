SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'refuelingStop' AS refuelingStop,
        data->>'_refuelingStop' AS _refuelingStop
    FROM events
    WHERE data->>'refuelingStop' IS NOT NULL