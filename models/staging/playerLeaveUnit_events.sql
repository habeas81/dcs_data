SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'playerLeaveUnit' AS playerLeaveUnit,
        data->>'_playerLeaveUnit' AS _playerLeaveUnit
    FROM events
    WHERE data->>'playerLeaveUnit' IS NOT NULL