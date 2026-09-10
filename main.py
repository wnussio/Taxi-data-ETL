import os
from pyspark.sql import SparkSession
from transform import TaxiTransformer
from load import PostgresLoader
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_PATH = os.path.join(SCRIPT_DIR, "data", "raw", "yellow_tripdata_2024-01.parquet")

load_dotenv()
db_password = os.environ.get("DB_PASSWORD")
DB_CONNECTION_STRING = f"postgresql+psycopg2://postgres:{db_password}@localhost:5432/Testy"


spark = SparkSession.builder.appName("TaxiETL").getOrCreate()

# --- Transform ---
transformer = TaxiTransformer(spark)

bronze = transformer.load_bronze(RAW_PATH)
silver_final, rejected_all = transformer.clean_to_silver(bronze)
gold = transformer.aggregate_to_gold(silver_final)
rejected_summary = transformer.summarize_rejections(rejected_all)

gold.show(10)
rejected_summary.show()

# --- Load ---
loader = PostgresLoader(DB_CONNECTION_STRING)
loader.save_gold(gold)
loader.save_rejected_summary(rejected_summary)

spark.stop()