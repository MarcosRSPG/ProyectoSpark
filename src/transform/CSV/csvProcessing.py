from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, lit, current_timestamp
import os

spark = SparkSession.builder \
    .appName("CSVProcessing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

s3_input_path = "s3a://data-lake/csv/part-00000-fb0f469c-485e-4958-8a7e-9d9e9906d4a5-c000.csv"
s3_output_path = "s3a://data-lake/csv_procesed"

df = spark.read.option('header', False).option("delimiter", ",").csv(s3_input_path)

df.show(5)

invalid_pattern = r"^\w+_ERROR$"

def filter_invalid_values(df, columns, invalid_pattern):
    for col_name in columns:
        df = df.filter(~(col(col_name).rlike(invalid_pattern) | 
                         col(col_name).isin("None", "NULL") | 
                         (col(col_name) == "")))
    return df

def replace_with_mean(df, columns, invalid_pattern):
    for col_name in columns:
        avg_value = df.select(mean(col(col_name)).alias("mean_value")).collect()[0]["mean_value"]
        df = df.withColumn(
            col_name, 
            when(df.withColumn(
                col_name, 
                when(col(col_name).rlike(invalid_pattern) | col(col_name).isin("None", "NULL", ""), avg_value)
                .otherwise(col(col_name))
            )) 
        )
    return df

def replace_with_mode(df, columns, invalid_pattern):
    for col_name in columns:
        mode_value = df.groupBy(col_name).count().orderBy(col("count"), ascending=False).first()[0]
        df = df.withColumn(
                col_name, 
                when(col(col_name).rlike(invalid_pattern) | col(col_name).isin("None", "NULL", ""), mode_value)
                .otherwise(col(col_name))
            )     
    return df

df = filter_invalid_values(df, ['_c1', '_c2'], invalid_pattern)

df = replace_with_mean(df, ['_c3', '_c4'], invalid_pattern)

df = replace_with_mode(df, ['_c0'], invalid_pattern)


df = df.withColumn("Tratado", lit(True))


df = df.withColumn("Fecha_Insercion_UTC", current_timestamp())

df.show(200)

df.write.mode("overwrite").option("header", True).csv(s3_output_path)
print("Archivo tratado guardado en S3.")

spark.stop()