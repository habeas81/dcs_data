SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'engineShutdown' AS engineShutdown
        FROM events
        WHERE data->>'engineShutdown' IS NOT NULL