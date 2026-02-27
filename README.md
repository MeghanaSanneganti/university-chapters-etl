# University Chapters ETL Pipeline

## Project Overview

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline built using Python and Google BigQuery.

The pipeline extracts university chapter data from a public ArcGIS REST API, loads the raw JSON data into BigQuery, and transforms it into a clean analytical table using SQL.

This project showcases API integration, cloud data warehousing, SQL transformations, and structured data pipeline development.

---

## Architecture

ArcGIS REST API  
↓  
Python (requests + BigQuery Client)  
↓  
BigQuery Raw Table (du_dataset.du_chapters_raw)  
↓  
SQL Transformation  
↓  
BigQuery Clean Table (du_dataset.du_chapters_clean)

---

##  Technologies Used

- Python
- Google Cloud BigQuery
- SQL (BigQuery Standard SQL)
- REST API (ArcGIS)
- Data Warehousing Concepts

---

##  ETL Process

###  Extract
- Fetches university chapter data from the ArcGIS public API.
- Retrieves nested JSON containing attributes and geometry.

### Load (Raw Layer)
- Loads raw JSON data into:
  
  `du_dataset.du_chapters_raw`

- Uses BigQuery load jobs with schema autodetection.

###  Transform
- Flattens nested JSON structure using `UNNEST`
- Extracts relevant fields:
  - Object ID
  - University Chapter
  - City
  - State
  - Chapter ID
  - Regional Director
  - Longitude & Latitude
- Creates analytical table:

  `du_dataset.du_chapters_clean`



## BigQuery Tables

### Raw Table
`du_dataset.du_chapters_raw`

Stores raw JSON data exactly as received from the API.

### Clean Table
`du_dataset.du_chapters_clean`

Flattened and structured table ready for analysis and reporting.


## How to Run

 Place `key.json` in the project root. 
 
 Activate your virtual environment:  
 venv\Scripts\activate   # Windows
 
 Set Google credentials:
 set GOOGLE_APPLICATION_CREDENTIALS=key.json  # Windows
 
 Run the ETL script:
 python du_chapters_etl.py

 ---

## Screenshots

### Raw Data in BigQuery
![Raw Data Screenshot](screenshots/raw_data.png)

### Clean Data in BigQuery
![Clean Data Screenshot](screenshots/clean_data.png)

### SQL Transformation
![SQL Transformation Screenshot](screenshots/sql_transformation.png)
