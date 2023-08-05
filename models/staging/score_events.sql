SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'score' AS score
        FROM events
        WHERE data->>'score' IS NOT NULL