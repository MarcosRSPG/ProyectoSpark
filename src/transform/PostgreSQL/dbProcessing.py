from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, lit, current_timestamp

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

stores= 'store_ID'
name= 'store_name'
location= 'location'
demographics= 'demographics'
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

bucket_path = "s3a://data-lake/postgres/*.csv"
df = spark.read.option('header', 'true').option("delimiter", ",").csv(bucket_path)

invalid_values = ["", "STORE_ERROR", "LOCATION_ERROR", "DEMOGRAPHICS_ERROR"]

df_filtered_name = df.filter(~(df[name].isin(invalid_values) | df[name].isNull() | (df[name] == 'None')))
df_count_name = df_filtered_name.groupBy(name).count()
most_frequent_name = df_count_name.orderBy(col('count').desc()).first()
most_frequent_name_value = most_frequent_name[name]

df_filtered_location = df.filter(~(df[location].isin(invalid_values) | df[location].isNull() | (df[location] == 'None')))
df_count_location = df_filtered_location.groupBy(location).count()
most_frequent_location = df_count_location.orderBy(col('count').desc()).first()
most_frequent_location_value = most_frequent_location[location]

df_filtered_demographics = df.filter(~(df[demographics].isin(invalid_values) | df[demographics].isNull() | (df[demographics] == 'None')))
df_count_demographics = df_filtered_demographics.groupBy(demographics).count()
most_frequent_demographics = df_count_demographics.orderBy(col('count').desc()).first()
most_frequent_demographics_value = most_frequent_demographics[demographics]

df = df.withColumn(tratado, when(df[name].isin(invalid_values) | df[name].isNull() | (df[name] == 'None') | df[location].isin(invalid_values) | df[location].isNull() | (df[location] == 'None') | df[demographics].isin(invalid_values) | df[demographics].isNull() | (df[demographics] == 'None'), True).otherwise(False))

df = df.filter(~(df[stores].isin(invalid_values) | df[stores].isNull() | (df[stores] == 'None')))
df = df.withColumn(name, when(df[name].isin(invalid_values) | df[name].isNull() | (df[name] == 'None'), most_frequent_name_value).otherwise(df[name]))
df = df.withColumn(location, when(df[location].isin(invalid_values) | df[location].isNull() | (df[location] == 'None'), most_frequent_location_value).otherwise(df[location]))
df = df.withColumn(demographics, when(df[demographics].isin(invalid_values) | df[demographics].isNull() | (df[demographics] == 'None'), most_frequent_demographics_value).otherwise(df[demographics]))

df = df.withColumn(fecha_insercion, current_timestamp())

df = df.dropDuplicates()

df = df.withColumn(stores, col(stores).cast("int")) \
       .withColumn(name, col(name).cast("string")) \
       .withColumn(location, col(location).cast("string")) \
       .withColumn(demographics, col(demographics).cast("string")) \

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
    .csv(path='s3a://data-lake/db_processed', sep=',')