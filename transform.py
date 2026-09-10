from pyspark.sql import functions as F
class TaxiTransformer:
    def __init__(self, spark):
        self.spark = spark

    def load_bronze(self, path):
        bronze = self.spark.read.parquet(path)
        print("Bronze wczytane, liczba wierszy:", bronze.count())
        return bronze

    def clean_to_silver(self, bronze_df):
        bronze_z_czasem = bronze_df.withColumn(
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

        percentyl_995 = silver.approxQuantile("fare_amount", [0.995], 0.01)
        prog_ceny = percentyl_995[0]

        silver_final = silver.filter(silver['fare_amount'] <= prog_ceny)
        rejected_high_price = silver.filter(silver['fare_amount'] > prog_ceny) \
            .withColumn("reason", F.lit("high_price"))

        rejected_all = rejected_basic.unionByName(rejected_high_price)

        return silver_final, rejected_all

    def aggregate_to_gold(self, silver_df):
        gold = silver_df.withColumn("godzina", F.hour("tpep_pickup_datetime"))

        gold = gold.groupBy("godzina", "PULocationID").agg(
            F.count("*").alias("liczba_kursow"),
            F.avg('trip_duration_min').alias("avg_duration_min"),
            F.avg('trip_distance').alias("avg_distance"),
            F.avg('fare_amount').alias("avg_fare"),
            F.sum('trip_duration_min').alias("total_duration_min"),
            F.sum('trip_distance').alias("total_distance"),
            F.sum('fare_amount').alias("total_fare")
        )
        return gold

    def summarize_rejections(self, rejected_df):
        return rejected_df.groupBy("reason").count().withColumnRenamed("count", "liczba")