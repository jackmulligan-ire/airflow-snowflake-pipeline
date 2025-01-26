import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
import snowflake.connector as sc

dataset_file = "yellow_tripdata_2024-09.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")

BUCKET = os.environ.get("GCP_GCS_BUCKET")

def upload_to_gcs(bucket, object_name, local_file_path):
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file_path)

def create_table_from_stage():
    conn_params = {
        'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
        'user': os.environ.get('SNOWFLAKE_USER'),
        'role': 'ACCOUNTADMIN',
        'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
        'private_key_file': os.environ.get('SNOWFLAKE_PRIVATE_KEY_FILE'),
        'database': os.environ.get('SNOWFLAKE_DATABASE'),
        'schema': os.environ.get('SNOWFLAKE_SCHEMA'),
    }

    ctx = sc.connect(**conn_params)
    with ctx.cursor() as cs:
        cs.execute(f"""
            CREATE TABLE "terraform_demo_448321_db"."terraform_demo_448321_schema"."YELLOW_TRIP_DATA_2024_09"
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
            LOCATION=>'@"terraform_demo_448321_db"."terraform_demo_448321_schema"."terraform_demo_448321_external_stage"/raw/{dataset_file}'
            , FILE_FORMAT=>'"terraform_demo_448321_db"."terraform_demo_448321_schema"."terraform_demo_448321_file_format"'
                    )
                )
            );
        """)
        cs.execute(f"""
            COPY INTO "terraform_demo_448321_db"."terraform_demo_448321_schema"."YELLOW_TRIP_DATA_2024_09"
            FROM '@"terraform_demo_448321_db"."terraform_demo_448321_schema"."terraform_demo_448321_external_stage"/raw/yellow_tripdata_2024-09.parquet'
            FILE_FORMAT = '"terraform_demo_448321_db"."terraform_demo_448321_schema"."terraform_demo_448321_file_format"'
            MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';
        """)

with DAG(
    dag_id="jack_data_ingestion_gcs_dag",
) as dag:
    download_parquet_task = BashOperator(
        task_id = "download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} -o {path_to_local_home}/{dataset_file}"
    )

    local_to_gcs_task = PythonOperator(
        task_id = "upload_to_gcs_task",
        python_callable = upload_to_gcs,
        op_kwargs = {
            "bucket": BUCKET,
            "object_name": f"raw/{dataset_file}",
            "local_file_path": f"{path_to_local_home}/{dataset_file}"
        }
    )

    create_table_task = PythonOperator(
        task_id = "create_table_task",
        python_callable = create_table_from_stage
    )

    download_parquet_task >> local_to_gcs_task >> create_table_task