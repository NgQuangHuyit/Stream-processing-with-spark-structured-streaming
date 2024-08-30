from pyspark.sql.functions import col, from_json, to_timestamp, when, window, sum
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Tumbling Window Demo") \
        .master("spark://spark-master:7077") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    logger = Log4j(spark)

    stock_schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Type", StringType()),
        StructField("Amount", IntegerType()),
        StructField("BrokerCode", StringType())
    ])

    kafka_source_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092") \
        .option("subscribe", "stock-trades") \
        .option("startingOffsets", "earliest") \
        .load()

    trade_df = kafka_source_df \
            .select(from_json(col("value").cast("string"), stock_schema).alias("value")) \
            .select("value.*") \
            .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("Buy", when(col("Type") == "BUY", col("Amount")).otherwise(0)) \
            .withColumn("Sell", when(col("Type") == "SELL", col("Amount")).otherwise(0))

    window_agg_df = trade_df \
        .groupby(window(col("CreatedTime"), "15 minute")) \
        .agg(sum("Buy").alias("TotalBuy"),
             sum("Sell").alias("TotalSell"))

    result_df = window_agg_df \
        .select(col("window.start").alias("start"),
                col("window.end").alias("end"),
                "TotalBuy",
                "TotalSell")

    # result_df.show()

    stream_query = result_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Tumbling Window Demo Started")
    stream_query.awaitTermination()
