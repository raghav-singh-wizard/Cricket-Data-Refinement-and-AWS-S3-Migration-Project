import json
import csv
import psycopg2
import pandas as pd
import boto3
from io import StringIO

# Load the JSON data
with open(r'C:\Users\91914\Downloads\1384420.json', 'r') as file:
    json_data = json.load(file)

# Flatten the JSON data to DataFrame
flat_data = []

for inning in json_data['innings']:
    for over in inning['overs']:
        for delivery in over['deliveries']:
            runs = delivery['runs']
            flat_data.append({
                'match': json_data['info']['teams'],
                'inning': '1',
                'over': over['over'],
                'batter': delivery['batter'],
                'bowler': delivery['bowler'],
                'non_striker': delivery['non_striker'],
                'runs_batter': runs['batter'],
                'extras': runs['extras'],
                'total': runs['total']
            })

# Convert flattened data to DataFrame
df = pd.DataFrame(flat_data)

# PostgreSQL database connection details
DB_NAME = 'Raghav'
DB_USER = 'postgres'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = '5432'

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Create a cursor object using the connection
cur = conn.cursor()

# Define the table name and column names
table_name = 'cricket_match_data'

# Create the table in PostgreSQL
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\
    match VARCHAR(255),\
    inning VARCHAR(10),\
    over VARCHAR(10),\
    batter VARCHAR(255),\
    bowler VARCHAR(255),\
    non_striker VARCHAR(255),\
    runs_batter INT,\
    extras INT,\
    total INT\
)"
cur.execute(create_table_query)
conn.commit()

# Insert data into the table using copy_from method (faster for bulk insert)
output = StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
cur.copy_from(output, table_name, null="", sep='\t')
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

# Upload DataFrame to S3 directly without creating a CSV file
bucket_name = 'cricketdataraghavproject'
access_key = 'YOUR_AWS_ACCESS_KEY'
secret_key = 'YOUR_AWS_SECRET_KEY'

s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

s3.put_object(Bucket=bucket_name, Key='cricket_data.csv', Body=csv_buffer.getvalue())
print('Data uploaded to S3 successfully.')
