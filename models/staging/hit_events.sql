SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'hit' AS hit
        FROM events
        WHERE data->>'hit' IS NOT NULL