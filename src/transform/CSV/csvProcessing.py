from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, lit, current_timestamp

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

date= 'date'
stores= 'store_ID'
products= 'product_ID'
quantity= 'quantity_Sold'
revenue= 'revenue'
tratado= 'Tratado'
fecha_insercion= 'Fecha Insercion'

spark = SparkSession.builder \
    .appName("csvTransformData") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/transform/CSV/postgresql-42.7.3.jar:/opt/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark-apps/transform/CSV/postgresql-42.7.3.jar:/opt/spark/jars/*") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

bucket_path = "s3a://data-lake/csv/*.csv"
df = spark.read.option('header', 'true').option("delimiter", ",").csv(bucket_path)

invalid_values = ["", "STORE_ERROR", "PRODUCT_ERROR", "QUANTITY_ERROR", "REVENUE_ERROR", "DATE_ERROR"]

df_filtered = df.filter(~(df[date].isin(invalid_values) | df[date].isNull() | (df[date] == 'None')))
df_count = df_filtered.groupBy(date).count()
most_frequent_datetime = df_count.orderBy(col('count').desc()).first()
most_frequent_datetime_value = most_frequent_datetime[date]

quantity_mean = int(df.select(mean(col(quantity))).collect()[0][0])
revenue_mean = df.select(mean(col(revenue))).collect()[0][0]

df = df.withColumn(tratado, when(df[revenue].isin(invalid_values) | df[revenue].isNull() | (df[revenue] == 'None') | df[quantity].isin(invalid_values) | df[quantity].isNull() | (df[quantity] == 'None') | df[date].isin(invalid_values) | df[date].isNull() | (df[date] == 'None'), True).otherwise(False))

df = df.withColumn(date, when(df[date].isin(invalid_values) | df[date].isNull() | (df[date] == 'None'), most_frequent_datetime_value).otherwise(df[date]))
df = df.filter(~(df[stores].isin(invalid_values) | df[stores].isNull() | (df[stores] == 'None')))
df = df.filter(~(df[products].isin(invalid_values) | df[products].isNull() | (df[products] == 'None')))
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

df = df.withColumn(date, col(date).cast("timestamp")) \
       .withColumn(stores, col(stores).cast("int")) \
       .withColumn(products, col(products).cast("string")) \
       .withColumn(quantity, col(quantity).cast("float")) \
       .withColumn(revenue, col(revenue).cast("float"))

df.show()
df.printSchema()

df \
    .write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres-db:5432/processed_data") \
    .option("dbtable", "public.csv_data") \
    .option("user", "postgres") \
    .option("password", "casa1234") \
    .option("driver", "org.postgresql.Driver") \
    .save()