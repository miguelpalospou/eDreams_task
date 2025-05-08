from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 7),
}

with DAG(
    dag_id='etl_provider_bookings',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL pipeline for provider_bookings',
) as dag:

    # STEP 1: Create raw table to hold unvalidated CSV data
    create_raw_table = SQLExecuteQueryOperator(
        task_id='create_raw_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS test_search.provider_bookings_raw (
            provider_booking_id TEXT,
            booking_id TEXT,
            booking_timestamp TEXT,
            adults TEXT,
            children TEXT,
            infants TEXT,
            departure_geonode_id TEXT,
            arrival_geonode_id TEXT,
            booking_price TEXT,
            dep_date TEXT,
            is_edreams_merchant TEXT,
            booking_currency TEXT
        );
            """
    )

    # STEP 2: Load CSV into raw table using psql COPY 
    load_csv = BashOperator(
        task_id='load_csv',
        bash_command="""
            PGPASSWORD=airflow psql -h postgres -U airflow -d airflow -c "
            COPY test_search.provider_bookings_raw
            FROM '/opt/airflow/include/data/provider_booking.csv'
            DELIMITER ',' CSV HEADER;"
        """
    )

    # STEP 3: Clean the data - remove duplicates and null keys
    clean_data = SQLExecuteQueryOperator(
        task_id='clean_data',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS test_search.provider_bookings_cleaned AS
        SELECT DISTINCT
            provider_booking_id::TEXT,  -- or ::TEXT if not binary
            booking_id::TEXT,           -- or ::TEXT if not binary
            booking_timestamp::timestamp,
            adults::NUMERIC,
            children::NUMERIC,
            infants::NUMERIC,
            departure_geonode_id::NUMERIC,
            arrival_geonode_id::NUMERIC,
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(booking_price, '[^0-9.,-]', '', 'g'),
                    ',', '', 'g'
                ) AS NUMERIC
            ) AS booking_price,
            dep_date::TEXT,
            is_edreams_merchant::TEXT,
            booking_currency::TEXT
        FROM test_search.provider_bookings_raw
        WHERE provider_booking_id IS NOT NULL AND booking_id IS NOT NULL;
        """
    )
    
    create_final_table = SQLExecuteQueryOperator(
        task_id='create_final_table',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS test_search.provider_booking (
                provider_booking_id TEXT,
                booking_id TEXT,
                booking_timestamp TIMESTAMP,
                adults NUMERIC,
                children NUMERIC,
                infants NUMERIC,
                departure_geonode_id NUMERIC,
                arrival_geonode_id NUMERIC,
                booking_price NUMERIC,
                dep_date TEXT,
                is_edreams_merchant TEXT,
                booking_currency TEXT
            );
        """
    )

    insert_cleaned_data = SQLExecuteQueryOperator(
        task_id='insert_cleaned_data',
        conn_id='postgres_default',
        sql="""
            INSERT INTO test_search.provider_booking (
                provider_booking_id,
                booking_id,
                booking_timestamp,
                adults,
                children,
                infants,
                departure_geonode_id,
                arrival_geonode_id,
                booking_price,
                dep_date,
                is_edreams_merchant,
                booking_currency
            )
            SELECT *
            FROM test_search.provider_bookings_cleaned cleaned
            WHERE NOT EXISTS (
                SELECT 1
                FROM test_search.provider_booking existing
                WHERE cleaned.provider_booking_id = existing.provider_booking_id
            );
        """
    )

    # Define DAG flow
create_raw_table >> load_csv >> clean_data >> create_final_table >> insert_cleaned_data
