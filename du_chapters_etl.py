import requests
from google.cloud import bigquery
import json

# ------------------------
# CONFIG
# ------------------------
API_URL = "https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/UniversityChapters_Public/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"
PROJECT_ID = "du-university-etl"  # Your GCP project ID
DATASET = "du_dataset"
RAW_TABLE = "du_chapters_raw"
CLEAN_TABLE = "du_chapters_clean"

# ------------------------
# STEP 1: Fetch data from API
# ------------------------
response = requests.get(API_URL)
data = response.json()
print(f"Fetched {len(data['features'])} records from API")

# ------------------------
# STEP 2: Load raw data into BigQuery
# ------------------------
client = bigquery.Client(project=PROJECT_ID)

table_id = f"{PROJECT_ID}.{DATASET}.{RAW_TABLE}"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
)

# Convert features to JSON lines for BQ
json_lines = [json.dumps(feature) for feature in data['features']]
load_job = client.load_table_from_json(json_lines, table_id, job_config=job_config)
load_job.result()  # Wait for completion
print(f"Loaded {len(json_lines)} records into {RAW_TABLE}")

# ------------------------
# STEP 3: Run SQL to create clean table
# ------------------------
sql = f"""
CREATE OR REPLACE TABLE {DATASET}.{CLEAN_TABLE} AS
SELECT
  feature.attributes.OBJECTID AS object_id,
  feature.attributes.University_Chapter AS university_chapter,
  feature.attributes.City AS city,
  feature.attributes.State AS state,
  feature.attributes.ChapterID AS chapter_id,
  feature.attributes.MEVR_RD AS regional_director,
  CAST(feature.geometry.x AS FLOAT64) AS longitude,
  CAST(feature.geometry.y AS FLOAT64) AS latitude
FROM
  {DATASET}.{RAW_TABLE},
  UNNEST(features) AS feature;
"""

query_job = client.query(sql)
query_job.result()
print(f"Clean table {CLEAN_TABLE} created successfully!")
