from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from minio import Minio

client = Minio("minio:9000", access_key="minio", secret_key="miniosecret", secure=False)
if not client.bucket_exists("demobucket1"):
    client.make_bucket("demobucket1")

MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'miniosecret'
MINIO_HOST = 'minio:9000'

def load_minio_config(sparkContext: SparkContext):
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_HOST)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Write to Minio") \
        .getOrCreate()
    load_minio_config(spark.sparkContext)
    df = spark.createDataFrame([("Alice", 34), ("Bob", 45), ("Charlie", 56)], ["name", "age"])
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save("s3a://demobucket1/data")

