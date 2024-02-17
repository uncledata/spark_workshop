from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Union

ROOT_DIR = '/home/iceberg/notebooks/PyCon_LT_Workshop/'
EXTRACTED_DATA_DIR = ROOT_DIR + 'data/'


def get_spark_session(session_name:str) -> SparkSession:
    return (SparkSession
         .builder
         .appName(session_name)
         .master("local[*]")
         .getOrCreate())

def get_yellow_taxi_data(spark:SparkSession) -> DataFrame:
    return spark.read.parquet(EXTRACTED_DATA_DIR+"*.parquet")
    
def get_dim_data(spark:SparkSession) -> Union[DataFrame, DataFrame, DataFrame, DataFrame]:
    dim_taxi_zones = spark.read.option("header",True).csv(EXTRACTED_DATA_DIR+"dim_taxi_zones.csv")
    dim_rates = spark.read.option("header",True).csv(EXTRACTED_DATA_DIR+"dim_rates.csv")
    dim_payments = spark.read.option("header",True).csv(EXTRACTED_DATA_DIR+"dim_payments.csv")
    dim_vendor = spark.read.option("header",True).csv(EXTRACTED_DATA_DIR+"dim_vendor.csv")
    return dim_taxi_zones, dim_rates, dim_payments, dim_vendor
    
