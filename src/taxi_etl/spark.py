import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str = "TaxiETL") -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.pop("SPARK_HOME", None)  
    return SparkSession.builder.appName(app_name).getOrCreate()
