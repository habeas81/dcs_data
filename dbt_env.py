from dotenv import load_dotenv
import os
import subprocess

# Load the .env file
load_dotenv()

# Print the DB_HOST to verify it's loaded correctly
print("DB_HOST:", os.getenv("DB_HOST"))

# Run the dbt command
subprocess.run(["dbt", "run"])
