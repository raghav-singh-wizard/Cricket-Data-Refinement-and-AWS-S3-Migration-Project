# Cricket Data Refinement and AWS S3 Migration Project

The Python script named `Cricket_Data_Refinement_and_AWS_S3_Migration_Project.py` converts preformatted JSON to a CSV file. The JSON source file `1384420.json` is transformed to `cricket_data.csv`. The primary objective is to clean the data for diverse insights and information.

## Goals:

- **Data Preparation**:
  Utilized Python to convert the JSON data into a more manageable format for subsequent analysis, using libraries like Pandas or built-in JSON libraries.

- **PostgreSQL Integration**:
  Configured PostgreSQL and established a connection using the `psycopg2` library to prepare the data for insertion.

- **AWS S3 Setup**:
  Set up an S3 bucket on AWS, ensuring proper configuration and accessibility.

- **PostgreSQL to AWS S3 Transfer**:
  Connected the local PostgreSQL database to AWS S3 via the `boto3` Python client and transferred the prepared data using access key and secret key authentication.

- **Databricks Implementation**:
  Established a connection between the S3 bucket and Databricks to process and transform the data within the Databricks environment.

- **Data Transformation in Databricks**:
  Performed necessary data manipulation within Databricks, using its functionalities for refining the data.

- **S3 Data Upload from Databricks**:
  After transformations, uploaded the cleaned and modified data back to the S3 bucket from Databricks.

- **Future Scope**:
  Potential data transfer from S3 to Amazon Redshift using AWS Lambda Function Trigger (not included in this project).

## Source Specifications:

The JSON data structure represents cricket match information with details such as players, runs, overs, and match outcomes.

The JSON DATA looks like this:-

```json
 {
              "batter": "MM Ali",
              "bowler": "Kuldeep Yadav",
              "non_striker": "LS Livingstone",
              "runs": {
                "batter": 1,
                "extras": 0,
                "total": 1
              }
            },
            {
              "batter": "LS Livingstone",
              "bowler": "Kuldeep Yadav",
              "non_striker": "MM Ali",
              "runs": {
                "batter": 0,
                "extras": 0,
                "total": 0
              }
```
## Transformation Process in Databricks:

Performed various operations in Databricks using PySpark:

- **File Mounting**:
  Connected to the S3 bucket using `dbutils` and displayed the directory path.

- **Data Loading and Display**:
  Read and displayed the cricket data in a tabular format.

- **Data Manipulation and Summary**:
  Conducted various data summarizations, including total runs by players, overwise runs, and inning-wise statistics.

- **Graph Visualization**:
  Created visualizations using Matplotlib to represent data insights graphically.

- **Data Save and Export**:
  Cleaned and transformed data was saved as a CSV file in the mounted S3 bucket.

##### DATABRICKS NOTEBOOK USED IN TRANSFORMATION:- 

```python
# S3 Bucket Mounting
ACCESS_KEY = "your_key"
SECRET_KEY = "your_secret_key"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")

dbutils.fs.mount(source=f"s3a://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@your_s3_bucket_name", 
                 mount_point="/mnt/anyname", 
                 extra_configs={"fs.s3a.connection.ssl.enabled": "false"})

# Path Display
display(dbutils.fs.ls("/mnt/cricket"))

# Data Display
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()

# Read the CSV file into a DataFrame
file_path = "/mnt/cricket/cricket_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the DataFrame
display(df)

# Data Transformation and Aggregation
from pyspark.sql import functions as F

# Calculate total runs played by each player
total_runs_by_player = df.groupBy('batter').agg(F.sum('runs_batter').alias('total_runs'))
display(total_runs_by_player)

# Calculate overwise runs
overwise_runs = df.groupBy('over').agg(F.sum('runs_batter').alias('total_runs')).toPandas()
import matplotlib.pyplot as plt

# Plotting the line graph for runs scored in each over
plt.plot(overwise_runs['over'], overwise_runs['total_runs'])
plt.xlabel('Over')
plt.ylabel('Runs Scored')
plt.title('Runs Scored over Overs')
plt.show()

# Save DataFrame to a CSV file in DBFS
file_path = "/dbfs/mnt/cricket/transformed_data.csv"
df.write.csv(file_path, header=True, mode='overwrite')

# Copy the transformed data to S3 bucket
dbutils.fs.cp("file:/dbfs/mnt/cricket/transformed_data.csv", "s3a://your_s3_bucket/")
```
## Challenges Faced:

- Outline challenges faced during data transformation and migration.
- Challenges such as data quality, data volume, and integration issues can be noted.

## Conclusion:

This project successfully accomplished the extraction, cleaning, transformation, and loading of cricket data, ready for further analysis or utilization. It demonstrated a comprehensive pipeline from data source refinement to AWS S3 migration and Databricks processing.

The project demonstrates a holistic workflow from data refinement to cloud migration and data processing, paving the way for further analytics and decision-making processes.
