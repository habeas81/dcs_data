SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'playerChangeSlot' AS playerChangeSlot
        FROM events
        WHERE data->>'playerChangeSlot' IS NOT NULL