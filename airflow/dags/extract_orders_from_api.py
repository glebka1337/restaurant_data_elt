import uuid
import pendulum
import requests
import json
import os
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from psycopg2.extras import Json
import psycopg2.errors # <-- ✅ ДОДАЙ ЦЕЙ РЯДОК
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- CONSTANTS ---
API_ID = 'restaurant_api'
POSTGRES_CONN_ID = "POSTGRES_DWH"
TARGET_SCHEMA = "raw_zone"
S3_CONN_ID = "S3_MINIO"
S3_BUCKET_VAR_NAME = "s3_bucket"

@dag(
    dag_id='restaurant_order_pipeline',
    start_date=pendulum.datetime(2025, 11, 11, tz='UTC'),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['elt', 'dbt', 's3', 'minio']
)
def restaurant_elt_pipeline():
    
    @task(task_id='extract_to_s3')
    def extract_orders_to_s3():
        try:
            api_url = Variable.get(API_ID)
        except KeyError:
            print(f'[ERROR] - Variable "{API_ID}" not found. Check .env and restart.')
            raise
        
        print('[INFO] - Calling api: {}'.format(api_url))
        try:
            response = requests.get(api_url, params={'batch_size': 100})
            response.raise_for_status()
            data = response.json()
            print(f'[INFO] - Extracted {len(data)} orders.')
            
            try:
                s3_bucket_name = Variable.get(S3_BUCKET_VAR_NAME)
            except KeyError:
                print(f"[ERROR] - Variable '{S3_BUCKET_VAR_NAME}' not found! Check airflow-init.")
                raise

            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

            print(f"[INFO] - Checking if bucket '{s3_bucket_name}' exists...")
            if not s3_hook.check_for_bucket(s3_bucket_name):
                print(f"[INFO] - Bucket not found. Creating bucket '{s3_bucket_name}'...")
                s3_hook.create_bucket(bucket_name=s3_bucket_name)
                print(f"[INFO] - Bucket '{s3_bucket_name}' created.")
            else:
                print(f"[INFO] - Bucket already exists.")

            s3_key = f"raw/orders/{uuid.uuid4()}.json"
            data_string = json.dumps(data)
            
            s3_hook.load_string(
                string_data=data_string,
                key=s3_key,
                bucket_name=s3_bucket_name,
                replace=True
            )
            
            print(f'[INFO] - Data saved to S3: s3://{s3_bucket_name}/{s3_key}')
            
            return {"s3_key": s3_key, "s3_bucket": s3_bucket_name}
            
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] - API call failed: {e}")
            raise
        
    @task(task_id='load_s3_to_postgres')
    def load_s3_to_db(s3_info: dict):
        
        context = get_current_context()
        real_run_id = context['ti'].run_id
        
        s3_key = s3_info['s3_key']
        s3_bucket = s3_info['s3_bucket']
        
        print(f"[INFO] - Found Airflow Run ID: {real_run_id}")
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
        print(f'[INFO] - Reading from S3: s3://{s3_bucket}/{s3_key}')
        
        data_string = s3_hook.read_key(key=s3_key, bucket_name=s3_bucket)
        orders_list = json.loads(data_string)
        
        print(f'[INFO] - {len(orders_list)} orders loaded from S3.')
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        

        try:

            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.staging_raw_orders (
                id SERIAL PRIMARY KEY, data JSONB NOT NULL,
                run_id VARCHAR(256), loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            
        except psycopg2.errors.UniqueViolation as e:
            print(f"[INFO] - Schema/Table already exists. (Caught {e}). Skipping creation.")
            conn.rollback() 
        
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] - DB Schema/Table creation failed: {e}")
            raise
        
        try:
            insert_sql = f"INSERT INTO {TARGET_SCHEMA}.staging_raw_orders (data, run_id) VALUES (%s, %s);"
            values = [(Json(order), real_run_id) for order in orders_list]
            cursor.executemany(insert_sql, values)
            conn.commit()
            print(f"[INFO] - Successfully inserted {len(orders_list)} rows into {TARGET_SCHEMA}.staging_raw_orders.")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] - DB Load (INSERT) failed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

        s3_hook.delete_objects(bucket=s3_bucket, keys=[s3_key])
        print(f'[INFO] - Staging file {s3_key} removed from S3. LOAD SUCCESS.')
        
    
    @task.virtualenv(
        task_id='transform_dbt_isolated',
        requirements=["dbt-postgres==1.8.2"], 
        system_site_packages=False 
    )
    def transform_dbt_in_venv():
        import os
        from dbt.cli.main import cli as dbt_cli

        print("[INFO] Running inside ISOLATED virtual environment...")
        
        os.environ['DBT_PROFILES_DIR'] = '/opt/airflow/'
        
        try:
            print("[INFO] Running dbt run...")
            dbt_cli.main(['run', '--project-dir', '/opt/airflow/dbt'])
            
        except SystemExit as e:
            if e.code == 0:
                print("[INFO] dbt run exited successfully (code 0).")
            else:
                print(f"[ERROR] dbt run failed with exit code {e.code}")
                raise
        
        print("[INFO] dbt transform complete.")

    s3_info_dict = extract_orders_to_s3()
    load_instance = load_s3_to_db(s3_info_dict)
    
    load_instance >> transform_dbt_in_venv()
    
restaurant_elt_pipeline()