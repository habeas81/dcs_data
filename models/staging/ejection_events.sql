SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'ejection' AS ejection
        FROM events
        WHERE data->>'ejection' IS NOT NULL