from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Streaming Join Demo") \
        .master("spark://spark-master:7077") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
        .getOrCreate()

    impressionSchema = StructType([
        StructField("InventoryID", StringType()),
        StructField("CreatedTime", StringType()),
        StructField("Campaigner", StringType()),
    ])

    clickSchema = StructType([
        StructField("InventoryID", StringType()),
        StructField("CreatedTime", StringType())
    ])

    kafka_impression_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092") \
        .option("subscribe", "impressions") \
        .option("startingOffsets", "earliest") \
        .load()

    impression_df = kafka_impression_df.select(from_json(col("value").cast("string"), impressionSchema).alias("value")) \
        .select(col("value.InventoryID").alias("ImpressionID"),
                col("value.CreatedTime").alias("CreatedTime"),
                col("value.Campaigner").alias("Campaigner"))

    kafka_click_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092") \
        .option("subscribe", "clicks") \
        .option("startingOffsets", "earliest") \
        .load()

    click_df = kafka_click_df.select(from_json(col("value").cast("string"), clickSchema).alias("value")) \
        .select(
        col("value.InventoryID").alias("ClickID"),
        col("value.CreatedTime").alias("CreatedTime")
    ) \
        .withColumn("ClickTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("CreatedTime")

    join_expr = impression_df.ImpressionID == click_df.ClickID
    join_type = "inner"

    joined_df = impression_df.join(click_df, join_expr, join_type)

    output_query = joined_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    output_query.awaitTermination()