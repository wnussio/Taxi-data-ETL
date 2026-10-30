"""
ETL pipeline dla danych NYC Taxi.
Bronze (wczytanie) -> Silver (czyszczenie + log odrzuceń) -> Gold (agregacja) -> PostgreSQL
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy import create_engine

# ====== KONFIGURACJA ======
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_PATH = os.path.join(SCRIPT_DIR, "data", "raw", "yellow_tripdata_2024-01.parquet")

DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_CONNECTION_STRING = f"postgresql+psycopg2://postgres:{DB_PASSWORD}@localhost:5432/Testy"

# ====== BRONZE: wczytanie ======
spark = SparkSession.builder.appName("TaxiETL").getOrCreate()

bronze = spark.read.parquet(RAW_PATH)
print("Bronze wczytane, liczba wierszy:", bronze.count())

# ====== SILVER: czyszczenie z audytowalnym powodem odrzucenia ======
bronze_z_czasem = bronze.withColumn(
    "trip_duration_min",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
)

oznaczone = bronze_z_czasem.withColumn(
    "reason",
    F.when(bronze_z_czasem['trip_duration_min'] < 3, "too_short")
     .when(bronze_z_czasem['trip_duration_min'] > 120, "too_long")
     .when(bronze_z_czasem['trip_distance'] <= 0, "bad_distance")
     .when(bronze_z_czasem['fare_amount'] <= 0, "bad_price")
     .otherwise(None)
)

silver = oznaczone.filter(oznaczone['reason'].isNull())
rejected_basic = oznaczone.filter(oznaczone['reason'].isNotNull())

print("Silver (przed percentylem):", silver.count())
print("Odrzucone (podstawowe warunki):", rejected_basic.count())

# percentyl 99.5 ceny, liczony na już wstępnie oczyszczonych danych
percentyl_995 = silver.approxQuantile("fare_amount", [0.995], 0.01)
prog_ceny = percentyl_995[0]
print(f"Próg ceny (99.5 percentyl): {prog_ceny}")

silver_final = silver.filter(silver['fare_amount'] <= prog_ceny)

rejected_high_price = silver.filter(silver['fare_amount'] > prog_ceny) \
    .withColumn("reason", F.lit("high_price"))

rejected_all = rejected_basic.unionByName(rejected_high_price)

print("Silver final:", silver_final.count())
print("Rejected all:", rejected_all.count())

# ====== GOLD: agregacja godzina x strefa ======
gold = silver_final.withColumn("godzina", F.hour("tpep_pickup_datetime"))

gold = gold.groupBy("godzina", "PULocationID").agg(
    F.count("*").alias("liczba_kursow"),
    F.avg('trip_duration_min').alias("avg_duration_min"),
    F.avg('trip_distance').alias("avg_distance"),
    F.avg('fare_amount').alias("avg_fare"),
    F.sum('trip_duration_min').alias("total_duration_min"),
    F.sum('trip_distance').alias("total_distance"),
    F.sum('fare_amount').alias("total_fare")
)

print("Gold:", gold.count())
gold.show(10)

# podsumowanie odrzuceń
rejected_summary = rejected_all.groupBy("reason").count().withColumnRenamed("count", "liczba")
rejected_summary.show()

# wgrywanie z pandas, zeby pominąć konfigurację javy/jar

gold_pd = gold.toPandas()
rejected_summary_pd = rejected_summary.toPandas()

engine = create_engine(DB_CONNECTION_STRING)

gold_pd.to_sql("gold_hourly_zone", engine, if_exists="replace", index=False)
rejected_summary_pd.to_sql("rejected_summary", engine, if_exists="replace", index=False)

print("Zapisano do PostgreSQL: gold_hourly_zone, rejected_summary")

spark.stop()