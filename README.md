# ✈️ ETL Pipeline – `provider_bookings` (Airflow DAG)

## 📋 Overview

This Airflow DAG (`etl_provider_bookings`) implements a complete ETL (Extract-Transform-Load) process to load, clean, and insert booking data from a CSV file (`provider_booking.csv`) into a PostgreSQL database table `test_search.provider_booking`.

The DAG:
1. Creates a staging table (`provider_bookings_raw`)
2. Loads raw data from a CSV using `psql COPY`
3. Cleans and transforms the data into correct types
4. Creates a cleaned production table (`provider_booking`) if it doesn't exist
5. Inserts new, non-duplicate records into the production table

![image](https://github.com/user-attachments/assets/dba4038b-c290-4403-a444-bbac8255df8e)


## How to Run

1. **Start the Docker Network (if not already running):**
   ```sh
   docker network create shared-network
   ```

2. **Start the Airflow stack:**
   ```sh
   cd docker-airflow
   docker-compose up -d
   ```

3. **Access Airflow UI:**
   - Open [http://localhost:8080](http://localhost:8080)
   - Login: `airflow` / `airflow`

4. **Trigger the DAG:**
   - The DAG is named `etl_provider_bookings`.
   - Trigger it manually from the Airflow UI.

5. **Exported Final Table:**
   - After the DAG finishes, the final table is exported as a CSV:
     ```
     data/provider_booking_final.csv
     ```
   - This file contains the cleaned, deduplicated, and type-correct data.

## Assumptions & Decisions
- I noticed some values were not correct in the input csv file. My decision was to include a regex mapping function in the transformation stage to eliminate non-numeric characters from the price fields.
However, some values in the date column were corrected manually, since there wasn't a clear pattern, and future input data could have different incorrect data.
- NOTE: I highly recommend to add a data consistancy check when the data is generated form the very beginning (CRMs, eDreams website, etc) to avoid these problems.
- NOTE 2: In the future I would add some testing checks at the raw data extraction. These are crucial to detect potential problems upstream. We could use "Great Expectations" here.
  
- I saw that the provider_bookings.csv file had an extra space at the beginnign of the file name. This is something that should be avoided, since it was causing extraction issues.
- All IDs are stored as `TEXT` for compatibility and simplicity.
- Deduplication is performed on the entire row for the raw data, meaning that when there are fully duplicated rows, they will be removed, this makes sense because we don't even need to ingest that data more likely.
- A second deduplication is carried out at final table load only on `provider_booking_id` with a logic to include only NEW booking_ids in the final table. This is something that should be discussed with the team.
- Data cleaning includes:
  - Removing non-numeric characters from price fields.
  - Converting timestamps to the correct format.
  - Handling missing or malformed data by setting fields to `NULL` where appropriate.
 
# Deliveries

## 1. Export of the Final Table
- **Path:** `data/provider_booking_final.csv`
- **Description:** Contains the final, cleaned, deduplicated table as required.

## 2. ETL Process Code
- **Path:** `docker-airflow/dags/etl_provider_bookings.py`
- **Description:** Airflow DAG with all SQL and orchestration logic.

## 3. Orchestrator Code
- **Path:** `docker-airflow/dags/etl_provider_bookings.py`
- **Description:** This file is used by Airflow to run the ETL pipeline. Trigger the DAG from the Airflow UI.

## 4. Docker Compose Files
- **Path:** `docker-airflow/docker-compose.yaml`
- **Description:** Used to start all required services (Airflow, Postgres, Redis, etc.).
- **How to run:**
  ```sh
  cd docker-airflow
  docker-compose up -d
  ```
## 3. QUERIES for Analysis 
- **Path:** `eDreams_task/analysis_queries.sql
- **Description:** This file contains the queries for task 2


## Rerunning the Pipeline

- To rerun the pipeline, simply trigger the DAG again in the Airflow UI.
- If you want to reload new data, replace the CSV in `data/provider_booking.csv` and rerun the DAG.

## Troubleshooting

- If you do not see the DAG in the UI, ensure the `dags/etl_provider_bookings.py` file is present and Airflow is running.
- If containers are not running, use `docker-compose up -d` to start them.

## Proposed architecture

![image](https://github.com/user-attachments/assets/c24bcd7e-47c7-48f5-9eaa-11adcb436032)
