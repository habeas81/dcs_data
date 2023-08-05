SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'takeoff' AS takeoff
        FROM events
        WHERE data->>'takeoff' IS NOT NULL