from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, lit, current_timestamp

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

date= '_c0'
stores= '_c1'
products= '_c2'
quantity= '_c3'
revenue= '_c4'
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

bucket_path = "s3a://data-lake/csv/part-00000-70dd3bbb-c1aa-441a-9449-473a8558142d-c000.csv"
df = spark.read.option('header', 'true').option("delimiter", ",").csv(bucket_path)

invalid_values = ["", "STORE_ERROR", "PRODUCT_ERROR", "QUANTITY_ERROR", "REVENUE_ERROR", "DATE_ERROR"]

df_filtered = df.filter(~(df[date].isin(invalid_values) | df[date].isNull() | (df[date] == 'None')))
df_count = df_filtered.groupBy(date).count()
most_frequent_datetime = df_count.orderBy(col('count').desc()).first()
most_frequent_datetime_value = most_frequent_datetime[date]

quantity_mean = df.select(mean(col(quantity))).collect()[0][0]
revenue_mean = df.select(mean(col(revenue))).collect()[0][0]

df = df.withColumn(tratado, when(df[revenue].isin(invalid_values) | df[revenue].isNull() | (df[revenue] == 'None') | df[quantity].isin(invalid_values) | df[quantity].isNull() | (df[quantity] == 'None') | df[date].isin(invalid_values) | df[date].isNull() | (df[date] == 'None'), True).otherwise(False))

df = df.withColumn(date, when(df[date].isin(invalid_values) | df[date].isNull() | (df[date] == 'None'), most_frequent_datetime_value).otherwise(df[date]))
df = df.filter(~(df[stores].isin(invalid_values) | df[stores].isNull() | (df[stores] == 'None')))
df = df.filter(~(df[products].isin(invalid_values) | df[products].isNull() | (df[products] == 'None')))
df = df.withColumn(quantity, when(df[quantity].isin(invalid_values) | df[quantity].isNull() | (df[quantity] == 'None'), quantity_mean).otherwise(df[quantity]))
df = df.withColumn(revenue, when(df[revenue].isin(invalid_values) | df[revenue].isNull() | (df[revenue] == 'None'), revenue_mean).otherwise(df[revenue]))

df = df.withColumn(fecha_insercion, current_timestamp())

df.show(200)

df \
    .write \
    .format('csv') \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://data-lake/csv_processed', sep=',')