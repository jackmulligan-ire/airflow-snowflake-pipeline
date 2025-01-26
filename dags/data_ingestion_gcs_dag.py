import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage

dataset_file = "yellow_tripdata_2024-09.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")

BUCKET = os.environ.get("GCP_GCS_BUCKET")

def upload_to_gcs(bucket, object_name, local_file_path):
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file_path)


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

    download_parquet_task >> local_to_gcs_task