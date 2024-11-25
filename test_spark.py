from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Jupyter Spark").getOrCreate()
data = spark.range(1, 100)
data.show()
