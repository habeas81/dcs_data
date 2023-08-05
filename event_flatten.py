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
            keys_set.update(data.keys())

    # Close the connection
    cursor.close()
    conn.close()

    return list(keys_set)


def create_sql_files(keys):
    # Path to the dbt models directory where the files will be created
    path_to_dbt_models = r"D:\GitHub\sdcs_data\sdcs_data\models\staging"

    # Create the view definition with renamed "time" column
    view_definition = """
    SELECT 
        id, 
        campaign_id, 
        initiator_user_id, 
        target_user_id, 
        time AS event_time,
    """
    
    for key in keys:
        # Add the key to the view definition
        view_definition += f"data->>'{key}' AS {key},\n"

    # Remove the last comma from the view definition
    view_definition = view_definition.rstrip(",\n")

    # Add the FROM clause and WHERE condition to the view definition
    view_definition += """
    FROM {{ ref('events') }}
    """

    for key in keys:
        filename = os.path.join(path_to_dbt_models, f"{key}_events.sql")
        with open(filename, "w") as file:
            # Remove the last semicolon from the view definition before writing to the file
            file.write(view_definition.rstrip(";"))

        print(f"File {filename} created.")


if __name__ == '__main__':
    keys = get_first_level_keys()
    print("First-level keys:", keys)
    create_sql_files(keys)
    print("All files created.")
