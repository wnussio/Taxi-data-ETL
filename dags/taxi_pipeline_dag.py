import sys
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.append("/opt/airflow/dags/taxi_etl")

from taxi_etl.config import Settings  # noqa: E402
from taxi_etl.extract import TaxiDataExtractor  # noqa: E402
from taxi_etl.pipeline import run_pipeline  # noqa: E402

# Configuration comes from the Airflow container environment, e.g.:
#   DB_HOST=host.docker.internal, LLM_HOST=host.docker.internal:1234,
#   DATA_DIR=/opt/airflow/dags/taxi_etl/data/raw, DB_PASSWORD, DB_NAME=Testy
# Settings.from_env() is called inside the tasks, not at module level,
# because Airflow parses this file over and over.


def task_extract():
    settings = Settings.from_env()
    extractor = TaxiDataExtractor(year=settings.taxi_year, month=settings.taxi_month)
    extractor.download(destination_folder=settings.data_dir)


def task_process_and_enrich():
    run_pipeline(Settings.from_env(), app_name="TaxiETL_Airflow")


with DAG(
    dag_id="taxi_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["taxi", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    process_and_enrich = PythonOperator(
        task_id="process_and_enrich",
        python_callable=task_process_and_enrich,
    )

    extract >> process_and_enrich
