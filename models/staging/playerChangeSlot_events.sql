SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'playerChangeSlot' AS playerChangeSlot,
        data->>'_playerChangeSlot' AS _playerChangeSlot
    FROM events
    WHERE data->>'playerChangeSlot' IS NOT NULL