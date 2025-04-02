from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, count, lit, current_timestamp, mode

aws_access_key_id = 'test'
aws_secret_access_key = 'test'



spark = SparkSession.builder \
    .appName("csvTransformData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Leer el archivo CSV desde el bucket
bucket_path = "s3a://data-lake/csv/part-00000-75c2e779-eb2c-450b-b65d-d43e2b05430c-c000.csv"
df = spark.read.option('header', 'true').option("delimiter", ",").csv(bucket_path)



df.show()