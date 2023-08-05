SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'markRemove' AS markRemove
        FROM events
        WHERE data->>'markRemove' IS NOT NULL