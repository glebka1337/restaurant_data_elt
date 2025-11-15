import requests
import uuid
import json
import psycopg2.errors 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from psycopg2.extras import Json

def extract_to_s3(
    api_url: str, 
    s3_conn_id: str, 
    s3_bucket_name: str, 
    s3_key_prefix: str,
    api_params: dict = None
) -> dict:
    
    print(f"[INFO] - Calling api: {api_url}")
    if api_params:
        print(f"[INFO] - With params: {api_params}")
        
    response = requests.get(api_url, params=api_params)
    response.raise_for_status()
    data = response.json()
    
    if not data:
        print("[INFO] - API returned no data. Skipping S3 Upload.")
        return None
        
    print(f'[INFO] - Extracted {len(data)} items.')
    
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    
    if not s3_hook.check_for_bucket(s3_bucket_name):
        print(f"[INFO] - Bucket not found. Creating bucket '{s3_bucket_name}'...")
        s3_hook.create_bucket(bucket_name=s3_bucket_name)
    else:
        print(f"[INFO] - Bucket already exists.")
    
    s3_key = f"{s3_key_prefix}/{uuid.uuid4()}.json"
    data_string = json.dumps(data)
    
    s3_hook.load_string(
        string_data=data_string,
        key=s3_key,
        bucket_name=s3_bucket_name,
        replace=True
    )
    
    print(f'[INFO] - Data saved to S3: s3://{s3_bucket_name}/{s3_key}')
    
    return {"s3_key": s3_key, "s3_bucket": s3_bucket_name}

def load_s3_to_postgres(
    s3_info: dict,
    config: dict,
    target_schema: str,
    target_table: str,
    load_method: str = 'append'
):
    
    if s3_info is None:
        print("[INFO] - No S3 info provided (upstream skipped). Skipping load.")
        return

    context = get_current_context()
    real_run_id = context['ti'].run_id
    
    s3_bucket = s3_info['s3_bucket']
    s3_key = s3_info['s3_key']
    
    s3_conn_id = config["S3_CONN_ID"]
    postgres_conn_id = config["POSTGRES_CONN_ID"]
    
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    print(f'[INFO] - Reading from S3: s3://{s3_bucket}/{s3_key}')
    data_string = s3_hook.read_key(key=s3_key, bucket_name=s3_bucket)
    data_list = json.loads(data_string)
    print(f'[INFO] - {len(data_list)} items loaded from S3.')

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try: 
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL,
            run_id VARCHAR(255) NOT NULL,
            loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """
        cursor.execute(create_table_sql)
        
        if load_method == 'truncate':
            print(f'[INFO] - TRUNCATE mode. Truncating table {target_schema}.{target_table}...')
            cursor.execute(f"TRUNCATE TABLE {target_schema}.{target_table};")
        else:
             print(f'[INFO] - APPEND mode. Skipping truncate.')

        insert_sql = f"INSERT INTO {target_schema}.{target_table} (data, run_id) VALUES (%s, %s)"
        vals = [(Json(item), real_run_id) for item in data_list]
        cursor.executemany(insert_sql, vals)
        conn.commit()
        
        print(f"[INFO] - Data loaded to DB. {len(vals)} rows inserted.")
        
    except psycopg2.errors.UniqueViolation as e:
        print(f"[INFO] - Schema/Table already exists. (Caught {e}). Skipping creation.")
        conn.rollback()
    except Exception as e:
        print(f"[ERROR] - Error while loading data to DB: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
    
    s3_hook.delete_objects(bucket=s3_bucket, keys=[s3_key])
    print(f"[INFO] - Staging file deleted from S3: s3://{s3_bucket}/{s3_key}")