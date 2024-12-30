from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from datetime import datetime, timedelta
import uuid

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024,12,29)
}

dag = DAG(
    "airflow-ci-cd-using-github",
    default_args=default_args,
    description="A CI/CD pipeline using Airflow and Github",
    catchup=False,
    tags=['dev'],
    schedule_interval=None
)

env = Variable.get("env", default_var="dev")
bq_project = Variable.get("bq_project", default_var="euphoric-diode-442615-u6")
bq_dataset = Variable.get("bq_dataset", default_var="flight_data_dev")
tables = Variable.get("tables", deserialize_json=True)

transformed_table = tables["transformed_table"]
route_insights_table = tables["route_insights_table"]
origin_insights_table = tables["origin_insights_table"]

batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity
bucket_name = "airflow_project_sankarsh"

file_sensor = GCSObjectExistenceSensor(
    task_id='check_input_data',
    bucket=bucket_name,
    object='airflow-ci-cd/data/flight_booking.csv',
    google_cloud_conn_id = "google_cloud_default",
    timeout = 300,
    poke_interval=30,  # Time between checks
    mode='poke',
    dag = dag
)

# Task 2: Submit PySpark job to Dataproc Serverless
batch_details = {
    "pyspark_batch": {
        "main_python_file_uri": f"gs://{bucket_name}/airflow-ci-cd/code/spark_transformation_job.py",  # Main Python file
        "python_file_uris": [],  # Python WHL files like if we want to use Pandas in the Spark Code we should download whl files and upload in the gcp and mention the path
        "jar_file_uris": [],  # JAR files
        "args": [
            f"--env={env}",
            f"--bq_project={bq_project}",
            f"--bq_dataset={bq_dataset}",
            f"--transformed_table={transformed_table}",
            f"--route_insights_table={route_insights_table}",
            f"--origin_insights_table={origin_insights_table}",
        ]
    },
    "runtime_config": {
        "version": "2.2",  # Specify Dataproc version (if needed)
    },
    "environment_config": {
        "execution_config": {
            "service_account": "340433674005-compute@developer.gserviceaccount.com",
            "network_uri": "projects/euphoric-diode-442615-u6/global/networks/default",
            "subnetwork_uri": "projects/euphoric-diode-442615-u6/regions/us-central1/subnetworks/default",
        }
    },
}

spark_submit = DataprocCreateBatchOperator(
    task_id = "SparkSubmit_ServerLess_Architecture",
    batch = batch_details,
    batch_id = batch_id,
    project_id = "euphoric-diode-442615-u6",
    region = "us-central1",
    gcp_conn_id="google_cloud_default",
    dag = dag
)

file_sensor >> spark_submit


