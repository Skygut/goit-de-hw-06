import datetime
import uuid

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

spark = (
    SparkSession.builder.appName("KafkaStreaming")
    .master("local[*]")
    .config("spark.sql.debug.maxToStringFields", "200")
    .config("spark.sql.columnNameLengthThreshold", "200")
    .getOrCreate()
)

alerts_df = spark.read.csv("alerts_conditions.csv", header=True)

window_duration = "1 minute"
sliding_interval = "30 seconds"

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )
    .option("subscribe", "building_sensors_volodymyr17")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "600")
    .load()
)


json_schema = StructType(
    [
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
    ]
)

avg_stats = (
    df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized",
        "CAST(value AS STRING) AS value_deserialized",
        "*",
    )
    .drop("key", "value")
    .withColumnRenamed("key_deserialized", "key")
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
    .withColumn(
        "timestamp",
        to_timestamp(
            col("value_json.timestamp"), "yyyy-MM-dd HH:mm:ss"
        ),  # Безпосереднє перетворення рядка у timestamp
    )
    .withWatermark("timestamp", "10 seconds")
    .groupBy(window(col("timestamp"), window_duration, sliding_interval))
    .agg(
        avg("value_json.temperature").alias("t_avg"),
        avg("value_json.humidity").alias("h_avg"),
    )
    .drop("topic")
)


all_alerts = avg_stats.crossJoin(alerts_df)

# query_all_alerts = (
#     all_alerts.writeStream.outputMode("complete")
#     .format("console")
#     .option("truncate", False)
#     .start()
# )


# try:
#     query_all_alerts.awaitTermination()
# except KeyboardInterrupt:
#     print("Стрим Kafka був примусово зупинений користувачем.")
# except Exception as e:
#     print(f"Стрим Kafka завершився з помилкою: {e}")
# finally:
#     # Зупинка стріму та Spark сесії
#     query_all_alerts.stop()
#     spark.stop()
#     print("Стрим Kafka та Spark сесія завершені.")


valid_alerts = (
    all_alerts.where("t_avg > temperature_min AND t_avg < temperature_max")
    .union(all_alerts.where("h_avg > humidity_min AND h_avg < humidity_max"))
    .withColumn("timestamp", current_timestamp())  # Використання динамічного timestamp
    .drop("id", "humidity_min", "humidity_max", "temperature_min", "temperature_max")
)


# # Для дебагінгу, перевіримо, що дані декодуються правильно
# query = (
#     valid_alerts.writeStream.outputMode("complete")
#     .format("console")
#     .option("truncate", False)
#     .start()
# )

# query.awaitTermination()


# from pyspark.sql.functions import col, to_json, struct, expr

# Використання monotonically_increasing_id() для створення ключа
prepare_to_kafka_df = valid_alerts.withColumn("key", expr("uuid()")).select(
    col("key"),
    to_json(
        struct(
            col("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            col("timestamp"),
        )
    ).alias("value"),
)

kafka_query = (
    prepare_to_kafka_df.writeStream.format("kafka")
    .outputMode("complete")  # записувати всі поточні результати у Kafka
    .option("kafka.bootstrap.servers", "77.81.230.104:9092")
    .option("topic", "alert_Kafka_topic")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';",
    )
    .option("checkpointLocation", "/tmp/checkpoints-3")
    .start()
    .awaitTermination()
)