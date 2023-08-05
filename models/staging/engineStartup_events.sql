
        SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'engineStartup' AS engineStartup
        FROM events
        