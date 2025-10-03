from pyspark.sql import SparkSession, functions as F # type: ignore
from pyspark.sql.functions import pandas_udf # type: ignore
import pandas as pd # type: ignore

spark = SparkSession.builder.appName("udf_pandas").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true").csv("raw_data/yellow_tripdata_2020-07.csv")

@F.udf("double")
def to_km_udf(miles):
    return miles * 1.60934 if miles is not None else None

df1 = df.withColumn("trip_km_udf", to_km_udf(F.col("trip_distance")))

@pandas_udf("double")
def to_km_pandas(miles: pd.Series) -> pd.Series:
    return miles * 1.60934

df2 = df.withColumn("trip_km_pandas", to_km_pandas(F.col("trip_distance")))

df2.select("trip_distance", "trip_km_pandas").show(5)
spark.stop()
