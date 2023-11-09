from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("CryptoOrderBook").getOrCreate()

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("symbol", StringType(), True),
    StructField("order_side", StringType(), True),
    StructField("size", DoubleType(), True),
    StructField("price", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", IntegerType(), True)
])

kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "technical_assessment") \
    .load()

orders_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("order")) \
    .select("order.*")

windowed_orders_df = orders_df.withColumn("created_at", F.from_unixtime(F.col("created_at")).cast("timestamp"))

watermarked_df = windowed_orders_df.withWatermark("created_at", "10 seconds")

order_book_df = watermarked_df.filter(col("status") == "OPEN")\
    .groupBy(window("created_at", "1 second"),
            "symbol", "order_side", "price", "order_id")\
    .agg(
        sum(col("size")).alias("amount"),
        sum(col("size") * col("price")).alias("total")
    )

format_df = order_book_df.select("symbol", concat_ws("_", "order_side", "order_id").alias("side"), "price", "amount", "total")
format_df = format_df.withColumn("order_book", struct(format_df.columns))




query = format_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# query = format_df.selectExpr("CAST(order_id AS STRING) as key",
#     "to_json(struct(order_book)) as value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "crypto_order_book") \
#     .option("checkpointLocation", "file:///tmp/checkpoints") \
#     .outputMode("append") \
#     .start()

query.awaitTermination()