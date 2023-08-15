import os

# Get the current script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define a function to write to a file
def write_to_file(folder, file_name, content):
    file_path = os.path.join(script_dir, '..', folder, file_name)
    with open(file_path, 'w') as file:
        file.write(content)
    print(f"SQL query written to {file_path}")

# Creating a unified structure for hit and kill events
hit_structure = """
WITH hits_combined AS (
    SELECT
        hit_id AS event_id,
        initiator_id AS initiator_id,
        initiator_name AS initiator_name,
        target_id AS target_id,
        target_name AS target_name,
        'hit' AS event_type,
        time_created::time AS event_time,
        weapon_type AS weapon_type
    FROM {{ ref('stg_sdcs__hits') }}
)
"""

kill_structure = """
, kills_combined AS (
    SELECT
        event_id AS event_id,
        initiator_id AS initiator_id,
        initiator_name AS initiator_name,
        target_id AS target_id,
        target_name AS target_name,
        'kill' AS event_type,
        time_created::time AS event_time,
        weapon_type AS weapon_type
    FROM {{ ref('stg_sdcs__kills') }}
)
"""

combine_hits_kills = f"""
{hit_structure}
{kill_structure}
SELECT * FROM hits_combined
UNION ALL
SELECT * FROM kills_combined
"""

write_to_file('models/intermediate', 'combined_hits_kills.sql', combine_hits_kills)

# Step 1: Identify Common Initiator and Target Pairs (eng pair)
identify_eng_pairs = f"""
WITH eng_pairs AS (
    SELECT
        initiator_id,
        initiator_name,
        target_id,
        target_name,
        ROW_NUMBER() OVER (ORDER BY initiator_id, target_id) AS pair_id
    FROM (
        {combine_hits_kills}
    ) combined_events
    GROUP BY initiator_id, initiator_name, target_id, target_name
)
SELECT * FROM eng_pairs
"""

write_to_file('models/intermediate', 'eng_pairs.sql', identify_eng_pairs)

# Step 2: Identify Engagements Based on Eng Pairs and Time Constraints
identify_engagements = f"""
WITH hits_combined AS (
    SELECT
        hit_id AS event_id,
        initiator_id AS initiator_id,
        initiator_name AS initiator_name,
        target_id AS target_id,
        target_name AS target_name,
        'hit' AS event_type,
        time_created::time AS event_time,
        weapon_type AS weapon_type
    FROM {{{{ ref('stg_sdcs__hits') }}}}
),
kills_combined AS (
    SELECT
        event_id AS event_id,
        initiator_id AS initiator_id,
        initiator_name AS initiator_name,
        target_id AS target_id,
        target_name AS target_name,
        'kill' AS event_type,
        time_created::time AS event_time,
        weapon_type AS weapon_type
    FROM {{{{ ref('stg_sdcs__kills') }}}}
),
combined_events_alias AS (
    SELECT * FROM hits_combined
    UNION ALL
    SELECT * FROM kills_combined
),
engagement_events AS (
    SELECT
        combined_events_alias.*,
        eng_pairs.pair_id AS pair_id
    FROM combined_events_alias
    JOIN {{{{ ref('eng_pairs') }}}}
    ON combined_events_alias.initiator_id = eng_pairs.initiator_id
    AND combined_events_alias.target_id = eng_pairs.target_id
)
, engagements AS (
    SELECT
        engagement_events.pair_id AS engagement_id, -- This alias
        MIN(event_id) AS start_event_id,
        MAX(event_id) AS end_event_id,
        engagement_events.pair_id AS pair_id -- Include this line
    FROM engagement_events
    GROUP BY engagement_events.pair_id
)
SELECT * FROM engagements
"""


write_to_file('models/intermediate', 'engagements.sql', identify_engagements)

# Step 3: Final Engagement View
final_engagements = f"""
WITH final_engagements AS (
    SELECT
        engagements.*,
        eng_pairs.initiator_id,
        eng_pairs.initiator_name,
        eng_pairs.target_id,
        eng_pairs.target_name
    FROM {{{{ ref('engagements') }}}}
    JOIN {{{{ ref('eng_pairs') }}}}
    ON engagements.pair_id = eng_pairs.pair_id
)
SELECT * FROM final_engagements
"""

write_to_file('analyses', 'final_engagements.sql', final_engagements)
