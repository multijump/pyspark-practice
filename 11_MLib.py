from pyspark.sql import SparkSession # type: ignore
from pyspark.ml.feature import VectorAssembler, StandardScaler # type: ignore
from pyspark.ml.classification import LogisticRegression # type: ignore
from pyspark.ml import Pipeline # type: ignore

spark = SparkSession.builder.appName("mlib").getOrCreate()

df = spark.createDataFrame([
    (0.0, 1.0, 0.0),
    (1.0, 2.0, 1.0),
    (2.0, 3.0, 0.0)
], ["f1", "f2", "label"])

assembler = VectorAssembler(inputCols=["f1", "f2"], outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[assembler, scaler, lr])

model = pipeline.fit(df)
print("Coefficients: ", model.stages[-1].coefficients)
spark.stop()
