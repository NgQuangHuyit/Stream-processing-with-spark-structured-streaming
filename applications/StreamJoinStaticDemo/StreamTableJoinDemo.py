from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from lib.logger import Log4j


def write_to_cassandra(target_df: DataFrame, batch_id):
    target_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "spark_db") \
        .option("table", "users") \
        .mode("append") \
        .save()
    target_df.show()


if  __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Streaming Join Demo") \
        .master("spark://spark-master:7077") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,com.datastax.spark:spark-cassandra-connector_2.12:3.5.2") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .getOrCreate()

    logger = Log4j(spark)

    login_schema = StructType([
        StructField("created_time", StringType()),
        StructField("login_id", StringType())
    ])

    kafka_source_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092") \
        .option("subscribe", "logins") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_source_df.select(from_json(col("value").cast("string"), login_schema).alias("value"))

    login_df = value_df.select("value.*") \
        .withColumn("created_time", to_timestamp(col("created_time"), "yyyy-MM-dd HH:mm:ss"))

    user_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "spark_db") \
        .option("table", "users") \
        .load()
    join_expr = login_df.login_id == user_df.login_id
    join_type = "inner"

    joined_df = login_df.join(user_df, join_expr, join_type) \
        .drop(login_df.login_id)

    output_df = joined_df.select(col("login_id"), col("user_name"),
                                 col("created_time").alias("last_login"))

    output_query = output_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    output_query.awaitTermination()