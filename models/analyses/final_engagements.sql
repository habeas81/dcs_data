WITH final_engagements AS (
    SELECT
        engagements.*,
        eng_pairs.initiator_id::text,
        eng_pairs.initiator_name,
        eng_pairs.target_id::text,
        eng_pairs.target_name
    FROM {{ ref('engagements') }}
    JOIN {{ ref('eng_pairs') }}
    ON engagements.engagement_id = eng_pairs.pair_id
)
SELECT * FROM final_engagements
