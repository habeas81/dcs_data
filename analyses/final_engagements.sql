
WITH final_engagements AS (
    SELECT
        engagements.*,
        eng_pairs.initiator_id,
        eng_pairs.initiator_name,
        eng_pairs.target_id,
        eng_pairs.target_name
    FROM {{ ref('engagements') }}
    JOIN {{ ref('eng_pairs') }}
    ON engagements.pair_id = eng_pairs.pair_id
)
SELECT * FROM final_engagements
