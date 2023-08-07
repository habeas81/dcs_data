SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'pilotDead' AS pilotDead,
        data->>'_pilotDead' AS _pilotDead
    FROM events
    WHERE data->>'pilotDead' IS NOT NULL