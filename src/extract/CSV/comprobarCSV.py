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

df = spark.read.parquet("s3a://data-lake/output/part-00000-d0f1b397-a34c-4b0c-b1d9-c838589cfde7-c000.csv")

df.show()

