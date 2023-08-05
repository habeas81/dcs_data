SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'markChange' AS markChange
        FROM events
        WHERE data->>'markChange' IS NOT NULL