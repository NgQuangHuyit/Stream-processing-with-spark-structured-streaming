from pyspark.sql.functions import from_json, col, expr, to_json
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

from lib.logger import Log4j
from lib.utils import load_minio_config

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Kafka Stream Demo") \
        .master("spark://spark-master:7077") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
        .getOrCreate()

    logger = Log4j(spark)
    load_minio_config(spark.sparkContext)
    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    # exploded_df = value_df.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.PosID",
    #                 "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
    #                 "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")
    logger.info("processing data")
    notification_df = value_df.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount") \
        .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2").cast(IntegerType()))
    kafka_target_df = notification_df.selectExpr("InvoiceNumber as key",
                                                 """
                                                 to_json(named_struct(
                                                    "CustomerCardNo", CustomerCardNo,
                                                    "TotalAmount", TotalAmount,
                                                    "EarnedLoyaltyPoints", EarnedLoyaltyPoints
                                                 )) as value
                                                 """)


    stream_query = kafka_target_df.writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092") \
        .option("topic", "notifications") \
        .option("checkpointLocation", "checkpoint") \
        .queryName("Notification Writer") \
        .trigger(processingTime="1 minute") \
        .start()

    stream_query.awaitTermination()
    spark.stop()