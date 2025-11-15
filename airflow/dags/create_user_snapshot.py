import pendulum
import os
from datetime import timedelta
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from utils.config_loader import get_pipeline_config
from utils.elt_tasks import extract_to_s3, load_s3_to_postgres

@dag(
    dag_id="user_snapshot_pipeline",
    start_date=pendulum.datetime(2025, 11, 11, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["elt", "dbt", "s3", "minio", "users"]
)
def user_snapshot_pipeline():

    @task(task_id="load_config")
    def load_config():
        return get_pipeline_config()

    @task(task_id="extract_users_snapshot")
    def extract_users_snapshot(config: dict):
        return extract_to_s3(
            api_url=config["API_USERS_URL"],
            s3_conn_id=config["S3_CONN_ID"],
            s3_bucket_name=config["S3_BUCKET_NAME"],
            s3_key_prefix="raw/user_snapshot",
            api_params=None
        )

    @task(task_id="load_users_snapshot")
    def load_users_snapshot(s3_info: dict, config: dict):
        if s3_info is None:
            print("[INFO] No data – skip load.")
            return
        load_s3_to_postgres(
            s3_info=s3_info,
            config=config,
            target_schema=config["SCHEMA_RAW_USERS"],
            target_table="staging_raw_users",
            load_method="truncate"
        )

    @task(task_id="export_dbt_env")
    def export_dbt_env() -> dict:
        return {
            "POSTGRES_USER":     Variable.get("POSTGRES_USER"),
            "POSTGRES_PASSWORD": Variable.get("POSTGRES_PASSWORD"),
            "POSTGRES_DB":       Variable.get("POSTGRES_DB"),
            "POSTGRES_HOST":     Variable.get("POSTGRES_HOST", "postgres-db"),
            "POSTGRES_PORT":     Variable.get("POSTGRES_PORT", "5432"),
            "DBT_PROFILES_DIR":  "/opt/airflow"
        }

    @task.virtualenv(
        task_id="transform_dbt_users_isolated",
        requirements=["dbt-postgres==1.8.2"],
        system_site_packages=False
    )
    def transform_dbt_users_isolated(env_vars: dict):
        import os
        from dbt.cli.main import cli as dbt_cli
        for k, v in env_vars.items():
            os.environ[k] = v
        
        try:
            dbt_cli.main(["run", "--project-dir", "/opt/airflow/dbt"])
        except SystemExit as e:
            if e.code != 0:
                raise
        return "dbt run finished"

    @task_group(group_id="elt_stage")
    def elt_group(config_data):
        s3_info = extract_users_snapshot(config_data)
        load_users_snapshot(s3_info, config_data)

    config_data = load_config()
    env_data    = export_dbt_env()
    elt_instance = elt_group(config_data)
    dbt_instance = transform_dbt_users_isolated(env_data)
    elt_instance >> dbt_instance

user_snapshot_pipeline()