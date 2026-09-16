import sys
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.append("/opt/airflow/dags/taxi_etl")

from extract import TaxiDataExtractor
from transform import TaxiTransformer
from load import PostgresLoader
from enrich import LLMEnricher


import os

DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_CONNECTION_STRING = f"postgresql+psycopg2://postgres:{DB_PASSWORD}@host.docker.internal:5432/Testy"LLM_BASE_URL = "http://host.docker.internal:1234"
LLM_MODEL = "qwen/qwen2.5-coder-14b"

DATA_FOLDER = "/opt/airflow/dags/taxi_etl/data/raw"
YEAR = 2024
MONTH = 1


def task_extract():
    extractor = TaxiDataExtractor(year=YEAR, month=MONTH)
    path = extractor.download(destination_folder=DATA_FOLDER)
    print(f"Dane dostępne pod: {path}")


def task_process_and_enrich():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("TaxiETL_Airflow").getOrCreate()

    try:
        extractor = TaxiDataExtractor(year=YEAR, month=MONTH)
        raw_path = extractor.get_local_path(destination_folder=DATA_FOLDER)

        transformer = TaxiTransformer(spark)

        bronze = transformer.load_bronze(raw_path)
        silver_final, rejected_all = transformer.clean_to_silver(bronze)
        gold = transformer.aggregate_to_gold(silver_final)
        rejected_summary = transformer.summarize_rejections(rejected_all)

        loader = PostgresLoader(DB_CONNECTION_STRING)
        loader.save_gold(gold)
        loader.save_rejected_summary(rejected_summary)

        llm_agg = transformer.aggregate_for_llm(silver_final)

        enricher = LLMEnricher(base_url=LLM_BASE_URL, model_name=LLM_MODEL)
        analiza = enricher.analyze_hourly_patterns(llm_agg)
        print(analiza)

        loader.save_llm_analysis(analiza)

    finally:
        spark.stop()


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

    #dziala w airflow!