import boto3
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", 'test') \
        .config("spark.hadoop.fs.s3a.secret.key", 'test') \
        .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

# Configure boto3 to use LocalStack endpoint
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',  # use the default access key
    aws_secret_access_key='test',  # use the default secret key
)

# Define the bucket name and object key
bucket_name = 'data-lake'
object_key = 'sales/part-00000-67c7e325-429c-4f2e-aceb-f908c87bf251-c000.json'

# Download the file from S3 bucket
response = s3.get_object(Bucket=bucket_name, Key=object_key)
data = response['Body'].read()

print(f"File '{object_key}' downloaded from s3://{bucket_name}/ whose values is {data}")
