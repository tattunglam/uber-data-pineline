## Introduction
The goal of this project is to perform data analytics on Uber data using various tools and technologies, including GCP Storage, Python, Compute Instance, Mage Data Pipeline Tool, BigQuery, and Looker Studio.
## Architecture
![image](https://github.com/tattunglam/uber-data-pipeline-with-gcp-mage/assets/44565384/a7ddad32-9147-495b-aa8d-34e662629eec)
1. Data Lake - **Cloud Storage**: to store raw data the dataset as a csv file.
2. Server - **Compute Engine**: to install & run **Mage** (ETL tool - an Airflow alternative). **Mage** execute python blocks to extract raw data from **Cloud Storage**, transform and load onto **BigQuery**.
3. Data Warehouse - **BigQuery**: to store transformed data that's ready for downstream usages.
4. BI/Visualization Tool - **Looker Studio**: to load data from BigQuery and build dashboard
## About Dataset
TLC Trip Record Data Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.
- Website - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Data Dictionary - https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
## References:
https://www.youtube.com/watch?v=WpQECq5Hx9g&t=517s
