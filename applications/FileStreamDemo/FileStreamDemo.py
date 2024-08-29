from pyspark.sql.functions import expr
from pyspark.sql.session import SparkSession

from lib.logger import Log4j
from lib.utils import load_minio_config

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Streaming Word Count") \
        .master("spark://spark-master:7077") \
        .config("spark.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    load_minio_config(spark.sparkContext)

    logger = Log4j(spark)

    raw_df = spark.readStream \
        .format("json") \
        .option("path", "s3a://demobucket/raw") \
        .option("maxFilesPerTrigger", "1") \
        .option("inferschema", "true") \
        .load()

    exploded_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID",
                      "PosID", "CustomerType", "PaymentMethod", "DeliveryType",
                      "DeliveryAddress.City", "DeliveryAddress.State", "DeliveryAddress.PinCode",
                      "explode(InvoiceLineItems) as LineItem")

    exploded_df.printSchema()

    flattened_df = exploded_df.withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    invoice_writer_query = flattened_df.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", "s3a://demobucket/invoices") \
        .option("checkpointLocation", "checkpoint") \
        .queryName("Flattened Invoice Writer") \
        .trigger(processingTime="1 minute") \
        .start()

    invoice_writer_query.awaitTermination()