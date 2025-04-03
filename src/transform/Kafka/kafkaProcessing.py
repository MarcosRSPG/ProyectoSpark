from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, current_timestamp

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

timestamp= 'timestamp'
stores= 'store_id'
products= 'product_id'
quantity= 'quantity_sold'
revenue= 'revenue'
tratado= 'Tratado'
fecha_insercion= 'Fecha Insercion'

spark = SparkSession.builder \
    .appName("csvTransformData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

bucket_path = "s3a://data-lake/sales_compacted/part-00000-2799b621-21df-4e5d-8a51-9e8827b2ff40-c000.json"
df = spark.read.json(bucket_path)

invalid_values = ["", "None", "STORE_ERROR", "PRODUCT_ERROR", "QUANTITY_ERROR", "REVENUE_ERROR", "TIMESTAMP_ERROR"]

df_filtered = df.filter(~(df[timestamp].isin(invalid_values) | df[timestamp].isNull() | (df[timestamp] == 'None')))
df_count = df_filtered.groupBy(timestamp).count()
most_frequent_timestamp = df_count.orderBy(col('count').desc()).first()
most_frequent_timestamp_value = most_frequent_timestamp[timestamp]

quantity_mean = int(df.select(mean(col(quantity))).collect()[0][0])
revenue_mean = df.select(mean(col(revenue))).collect()[0][0]

df = df.withColumn(tratado, when(df[revenue].isin(invalid_values) | df[revenue].isNull() | (df[revenue] == 'None') | df[quantity].isin(invalid_values) | df[quantity].isNull() | (df[quantity] == 'None') | df[timestamp].isin(invalid_values) | df[timestamp].isNull() | (df[timestamp] == 'None'), True).otherwise(False))
df = df.withColumn(timestamp, when(df[timestamp].isin(invalid_values) | df[timestamp].isNull() | (df[timestamp] == 'None'), most_frequent_timestamp_value).otherwise(df[timestamp]))

# Filtra filas donde store_id o product_id estén en la lista de valores inválidos o sean nulos
df = df.filter(~(df[stores].isin(invalid_values) | df[stores].isNull()))
df = df.filter(~(df[products].isin(invalid_values) | df[products].isNull()))

df = df.withColumn(quantity, when(df[quantity].isin(invalid_values) | df[quantity].isNull() | (df[quantity] == 'None'), quantity_mean).otherwise(df[quantity]))
df = df.withColumn(revenue, when(df[revenue].isin(invalid_values) | df[revenue].isNull() | (df[revenue] == 'None'), revenue_mean).otherwise(df[revenue]))

df = df.withColumn(fecha_insercion, current_timestamp())

df = df.dropDuplicates()

df = df.withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp")) \
       .withColumn(stores, col(stores).cast("int")) \
       .withColumn(products, col(products).cast("string")) \
       .withColumn(quantity, col(quantity).cast("int")) \
       .withColumn(revenue, col(revenue).cast("double"))

df.show()
df.printSchema()

df \
    .write \
    .format('csv') \
    .option('header', 'true') \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://data-lake/kafka_processed', sep=',')