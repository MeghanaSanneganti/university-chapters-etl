import requests
import json
from google.cloud import bigquery

# ------------------------
# CONFIG
# ------------------------
API_URL = "https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/UniversityChapters_Public/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"

PROJECT_ID = "du-university-etl"
DATASET = "du_dataset"
RAW_TABLE = "du_chapters_raw"
CLEAN_TABLE = "du_chapters_clean"


# ------------------------
# EXTRACT
# ------------------------
def extract_data():
    print("Fetching data from API...")
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data['features'])} records")
    return data


# ------------------------
# LOAD RAW DATA
# ------------------------
def load_raw_data(data):
    print("Loading raw data to BigQuery...")

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{RAW_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    json_lines = [json.dumps(feature) for feature in data["features"]]

    load_job = client.load_table_from_json(
        json_lines,
        table_id,
        job_config=job_config
    )

    load_job.result()
    print("Raw data loaded successfully.")


# ------------------------
# TRANSFORM DATA (SQL)
# ------------------------
def transform_data():
    print("Creating clean table...")

    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{CLEAN_TABLE}` AS
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
      `{PROJECT_ID}.{DATASET}.{RAW_TABLE}`,
      UNNEST(features) AS feature
    """

    query_job = client.query(query)
    query_job.result()

    print("Clean table created successfully.")


# ------------------------
# MAIN PIPELINE
# ------------------------
def main():
    data = extract_data()
    load_raw_data(data)
    transform_data()
    print("ETL Pipeline completed successfully!")


if __name__ == "__main__":
    main()
