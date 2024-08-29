from pyspark import SparkContext

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