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

def flatten_json(json_data, parent_key='', sep='_'):
    items = []
    for key, value in json_data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_json(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)

def get_first_level_keys():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Query to retrieve all JSONB data from the "data" column
    query = "SELECT data FROM public.events;"

    cursor.execute(query)
    records = cursor.fetchall()

    # Analyze the first-level keys in the JSONB data
    keys_set = set()
    for record in records:
        data = record[0]
        if isinstance(data, dict):  # Ensure that the data is a JSON object
            flattened_data = flatten_json(data)
            keys_set.update(flattened_data.keys())

    # Close the connection
    cursor.close()
    conn.close()

    return list(keys_set)

def create_sql_files(keys):
    # Path to the dbt models directory where the files will be created
    path_to_dbt_models = r"D:\GitHub\sdcs_data\sdcs_data\models\staging"

    for key in keys:
        # Create the view definition with renamed "time" column
        view_definition = f"""
        SELECT 
            id, 
            campaign_id, 
            initiator_user_id, 
            target_user_id, 
            time AS event_time,
            data->>'{key}' AS {key}
        FROM events
        WHERE data->>'{key}' IS NOT NULL
        """
        
        filename = os.path.join(path_to_dbt_models, f"{key}_events.sql")
        with open(filename, "w") as file:
            file.write(view_definition.strip())  # Removing leading and trailing whitespace

        print(f"File {filename} created.")

if __name__ == '__main__':
    keys = get_first_level_keys()
    print("First-level keys:", keys)
    create_sql_files(keys)
    print("All files created.")
