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

bucket_path = "s3a://data-lake/sales_compacted/*.json"
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

# Calcular Q1 y Q3 para el rango intercuartil (IQR) sin approxQuantile
q1_quantity, q3_quantity = df.selectExpr(
    f"percentile_approx({quantity}, 0.25)",
    f"percentile_approx({quantity}, 0.75)").first()

iqr_quantity = q3_quantity - q1_quantity
lower_bound_quantity = q1_quantity - 1.5 * iqr_quantity
upper_bound_quantity = q3_quantity + 1.5 * iqr_quantity

q1_revenue, q3_revenue = df.selectExpr(
    f"percentile_approx({revenue}, 0.25)",
    f"percentile_approx({revenue}, 0.75)").first()

iqr_revenue = q3_revenue - q1_revenue
lower_bound_revenue = q1_revenue - 1.5 * iqr_revenue
upper_bound_revenue = q3_revenue + 1.5 * iqr_revenue

# Filtrar valores atípicos
df = df.filter((col(quantity) >= lower_bound_quantity) & (col(quantity) <= upper_bound_quantity))
df = df.filter((col(revenue) >= lower_bound_revenue) & (col(revenue) <= upper_bound_revenue))

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