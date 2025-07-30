import snowflake.connector, os, pytz, re
from datetime import datetime, timedelta

# Connect to Snowflake using environment variables
con = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'])

cursor = con.cursor()

# Set current DB context
cursor.execute("USE DATABASE dev_analytics")

# Get list of all schemas
cursor.execute("SHOW SCHEMAS")

# Calculate date 30 days ago
thirty_days_ago = datetime.now(pytz.utc) - timedelta(days=30)

# Defines the pattern that the schema names should match
# shellcheck disable=SC2102
pattern = re.compile("DBT_.*_PR_\d+")

## Fetch all schemas
schemas = cursor.fetchall()

# Iterate through schemas
for schema in schemas:
    try:
        # Check if current schema matches naming pattern and was created over 30 days ago
        if pattern.match(schema[1]) and schema[0] < thirty_days_ago:
            # If schema matches criteria, execute DROP SCHEMA SQL command
            print(f"Dropping {schema[1]}")
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema[1]} CASCADE")
        else:
            print(f"Skipping {schema[1]}")
    except Exception as e:
        print(f"Error processing schema {schema[1]} due to {e}")

cursor.close() 
