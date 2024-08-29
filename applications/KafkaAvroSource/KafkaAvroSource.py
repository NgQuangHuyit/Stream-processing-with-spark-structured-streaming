from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, sum, expr, to_json, struct
from pyspark.sql.session import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Kafka Avro Source Demo") \
        .master("spark://spark-master:7077") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.apache.spark:spark-avro_2.12:3.5.2") \
        .getOrCreate()

    logger = Log4j(spark)

    kafka_source_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092") \
        .option("subscribe", "invoice-items") \
        .option("startingOffsets", "earliest") \
        .load()
    avroSchema = open("./schema/invoice-items", mode="r").read()
    value_df = kafka_source_df.select(from_avro(col("value"), avroSchema).alias("value"))

    rewards_df = value_df.filter("value.CustomerType == 'PRIME'") \
        .groupby("value.CustomerCardNo") \
        .agg(sum("value.TotalValue").alias("TotalPurchase"),
             sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))

    kafka_target_df = rewards_df.withColumn("key", col("CustomerCardNo")) \
        .withColumn("value", to_json(struct("TotalPurchase", "AggregatedRewards")))

    stream_query = kafka_target_df.writeStream \
        .queryName("Rewards Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092") \
        .option("topic", "customer-rewards") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("update") \
        .start()

    stream_query.awaitTermination()
