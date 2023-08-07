import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# PostgreSQL database connection details from environment variables
db_params = {
    "host": "localhost",
    "database": "sdcs",
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
}

def get_first_level_keys(view_name):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Query to retrieve all JSONB data from the "data" column
    query = f"SELECT data FROM {view_name};"

    cursor.execute(query)
    records = cursor.fetchall()

    # Analyze the first-level keys in the JSONB data
    keys_set = set()
    for record in records:
        data = record[0]
        if isinstance(data, dict):  # Ensure that the data is a JSON object
            keys_set.update(data.keys())

    # Close the connection
    cursor.close()
    conn.close()

    return list(keys_set)

def flatten_tree(view_name, parent_key, current_key, data, column_names, depth=1):
    # Base case: if the data is a JSON object, add the key to the column names
    if isinstance(data, dict):
        column_names.append(f"{parent_key}_{current_key}")
    else:
        # Recursive case: traverse the tree further
        for key, value in data.items():
            flatten_tree(view_name, current_key, key, value, column_names, depth + 1)

def create_sql_file(view_name, first_level_key, column_names):
    # Path to the dbt models directory where the file will be created
    path_to_dbt_models = r"D:\GitHub\sdcs_data\sdcs_data\models\staging"

    # Create the view definition with all the nested keys as columns
    view_definition = f"""
    SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
        data->>'{first_level_key}' AS {first_level_key},
        {', '.join(f"data->>'{column_name}' AS {column_name}" for column_name in column_names)}
    FROM {view_name}
    WHERE data->>'{first_level_key}' IS NOT NULL
    """
    
    # Create the .sql file for this view
    filename = os.path.join(path_to_dbt_models, f"{first_level_key}_events.sql")
    with open(filename, "w") as file:
        file.write(view_definition.strip())  # Removing leading and trailing whitespace

    print(f"File {filename} created.")

if __name__ == '__main__':
    view_name = "events"  # Modify this with the actual view name
    keys = get_first_level_keys(view_name)
    print("First-level keys:", keys)

    for first_level_key in keys:
        column_names = []
        flatten_tree(view_name, "", first_level_key, {}, column_names)
        print(f"Column names for {first_level_key}: {column_names}")
        create_sql_file(view_name, first_level_key, column_names)
    
    print("All files created.")
