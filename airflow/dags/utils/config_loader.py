from airflow.models import Variable

class ConfigKeys:
    API_ORDERS = 'restaurant_api'
    API_USERS_SNAPSHOT = 'restaurant_users_api'
    CONN_POSTGRES = 'POSTGRES_DWH'
    CONN_S3 = 'S3_MINIO'
    VAR_S3_BUCKET = 's3_bucket'
    SCHEMA_RAW_ORDERS = 'raw_zone'
    SCHEMA_RAW_USERS = 'raw_zone_users' 

def get_pipeline_config() -> dict:

    print("[INFO] Loading pipeline configuration from Airflow Variables...")
    try:
        config = {
            # Вытаскиваем 'Value' из Variables
            "API_ORDERS_URL": Variable.get(ConfigKeys.API_ORDERS),
            "API_USERS_URL": Variable.get(ConfigKeys.API_USERS_SNAPSHOT),
            "S3_BUCKET_NAME": Variable.get(ConfigKeys.VAR_S3_BUCKET),
            "S3_CONN_ID": ConfigKeys.CONN_S3,
            "POSTGRES_CONN_ID": ConfigKeys.CONN_POSTGRES,
            "SCHEMA_RAW_ORDERS": ConfigKeys.SCHEMA_RAW_ORDERS,
            "SCHEMA_RAW_USERS": ConfigKeys.SCHEMA_RAW_USERS,
        }
        print("[INFO] Configuration loaded successfully.")
        return config
    except KeyError as e:
        print(f"[FATAL] Missing required Airflow Variable: {e}. Check airflow-init!")
        raise