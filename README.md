# University Chapters ETL Project

## Project Description
This project demonstrates an ETL process using Google BigQuery.
Data was extracted from an ArcGIS REST API,
transformed using SQL, and loaded into a clean analytics-ready table.

## SQL File
The transformation query is saved as:

du_chapters_etl.sql

## Screenshots

Raw Data:  
![Raw Data](screenshots/raw_data.png)  

Clean Data:  
![Clean Data](screenshots/clean_data.png)  

SQL Query:  
![SQL Query](screenshots/sql_query.png)

## Run ETL Pipeline

1. Install dependencies: `pip install requests google-cloud-bigquery`
2. Set up GCP credentials (download service account JSON and set `GOOGLE_APPLICATION_CREDENTIALS`)
3. Run the ETL: `python etl_pipeline.py`
4. This will fetch data from the API, load into `du_chapters_raw`, and create `du_chapters_clean`
