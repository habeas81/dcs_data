WITH
combined_events AS (
    SELECT 'kill' AS event_type, * FROM {{ ref('stg_sdcs__kills') }}
    UNION ALL
    SELECT 'hit' AS event_type, * FROM {{ ref('stg_sdcs__hits') }}
    UNION ALL
    SELECT 'shot' AS event_type, * FROM {{ ref('stg_sdcs__shots') }}
)
SELECT * FROM combined_events
