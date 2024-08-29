from pyspark.sql.session import SparkSession
from pyspark.sql.functions import expr
from lib.logger import Log4j
# from lib.utils import load_minio_config

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Streaming Word Count") \
        .master("local[*]") \
        .config("spark.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

    # load_minio_config(spark.sparkContext)
    logger = Log4j(spark)
    logger.info("Spark Application Started")

    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "172.20.0.3") \
        .option("port", "9999") \
        .load()

    # lines_df.printSchema()
    words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
    counts_df = words_df.groupby("word").count()
    streaming_query = counts_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("complete") \
        .start()

    counts_df.printSchema()

    streaming_query.awaitTermination()