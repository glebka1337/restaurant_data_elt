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

# CONSTANTS
API_ID = 'restaurant_api' 
POSTGRES_CONN_ID = "POSTGRES_DWH" 
TARGET_SCHEMA = "raw_zone"

@dag(
    dag_id='restaurant_order_pipeline',
    start_date=pendulum.datetime(2025, 11, 11, tz='UTC'),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['elt', 'dbt']
)
def restaurant_elt_pipeline():
    
    @task(task_id='extract_new_orders')
    def extract_orders_from_api():
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
            file_path = f'/tmp/orders_{uuid.uuid4()}.json'
            with open(file_path, 'w') as f:
                json.dump(data, f)
            print(f'[INFO] - Data was saved to path: {file_path}')
            return file_path
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] - API call failed: {e}")
            raise
        
    @task(task_id='load_orders')
    def load_orders_in_db(file_path: str):
        context = get_current_context()
        real_run_id = context['ti'].run_id
        print(f"[INFO] - Found Airflow Run ID: {real_run_id}")
        with open(file_path, 'r')  as f:
            orders_list = json.load(f)
        print(f'[INFO] - {len(orders_list)} orders loaded from file.')
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")
            conn.commit()
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.staging_raw_orders (
                id SERIAL PRIMARY KEY, data JSONB NOT NULL,
                run_id VARCHAR(256), loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            insert_sql = f"INSERT INTO {TARGET_SCHEMA}.staging_raw_orders (data, run_id) VALUES (%s, %s);"
            values = [(Json(order), real_run_id) for order in orders_list]
            cursor.executemany(insert_sql, values)
            conn.commit()
            print(f"[INFO] - Successfully inserted {len(orders_list)} rows into {TARGET_SCHEMA}.staging_raw_orders.")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] - DB Load failed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
        os.remove(file_path)
        print(f'[INFO] - Staging file {file_path} removed. LOAD SUCCESS.')
        
    
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

    filepath_to_load = extract_orders_from_api()
    load_instance = load_orders_in_db(filepath_to_load)
    
    load_instance >> transform_dbt_in_venv()
    
restaurant_elt_pipeline()