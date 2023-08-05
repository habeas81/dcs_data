SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'playerSendChat' AS playerSendChat
        FROM events
        WHERE data->>'playerSendChat' IS NOT NULL