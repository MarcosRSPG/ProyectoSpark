from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, lit, current_timestamp, avg
import os
import re

spark = SparkSession.builder \
    .appName("CSVProcessing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

s3_input_path = "s3a://data-lake/csv/part-00000-5a744043-eaac-4b02-b8d7-01c613fa0f12-c000.csv"
s3_output_path = "s3a://data-lake/csv_procesed"

df = spark.read.option('header', False).option("delimiter", ",").csv(s3_input_path)

df.show(5)

invalid_pattern = r"^\w+_ERROR$"

df = df.withColumn("_c3", col("_c3").cast("double"))
df = df.withColumn("_c4", col("_c4").cast("double"))

def filter_invalid_values(df, columns, invalid_pattern):
    filtered_rdd = df.rdd.filter(lambda row: all(not (
        row[col_index] is None or 
        str(row[col_index]) == "" or 
        str(row[col_index]) in ["None", "NULL"] or 
        re.match(invalid_pattern, str(row[col_index]))
    ) for col_index in range(len(row)) if df.columns[col_index] in columns))
    
    return filtered_rdd.toDF(df.schema)  # Mantiene todas las columnas originales y su esquema

def replace_with_mean(df, columns, invalid_pattern):
    means = {}
    for col_name in columns:
        numeric_df = df.filter(col(col_name).rlike("^[0-9]+$"))
        mean_value = numeric_df.select(avg(col(col_name)).alias("mean")).collect()[0]["mean"]
        means[col_name] = float(mean_value) if mean_value is not None else 0

    df = df.rdd.map(lambda row: [
        means[col_name] if (df.schema[i].name in columns and (
            row[i] is None or 
            str(row[i]) == "" or 
            str(row[i]) in ["None", "NULL"] or 
            re.match(invalid_pattern, str(row[i]))
        )) else row[i]
        for i in range(len(row))
    ]).toDF(df.schema)  # Mantiene todas las columnas originales
    
    return df

def replace_with_mode(df, columns, invalid_pattern):
    modes = {}
    for col_name in columns:
        mode_row = (df.filter(~col(col_name).rlike(invalid_pattern))
                    .groupBy(col_name)
                    .agg(count("*").alias("count"))
                    .orderBy(desc("count"))
                    .limit(1)
                    .collect())
        
        modes[col_name] = mode_row[0][0] if mode_row else "0"

    df = df.rdd.map(lambda row: [
        modes[col_name] if (df.schema[i].name in columns and (
            row[i] is None or 
            re.match(invalid_pattern, str(row[i]))
        )) else row[i]
        for i in range(len(row))
    ]).toDF(df.schema)  # Mantiene todas las columnas originales
    
    return df

# Aplicar transformaciones
df = filter_invalid_values(df, ['_c1', '_c2'], invalid_pattern)
df = replace_with_mean(df, ['_c3', '_c4'], invalid_pattern)
df = replace_with_mode(df, ['_c0'], invalid_pattern)


df = df.withColumn("Tratado", lit(True))


df = df.withColumn("Fecha_Insercion_UTC", current_timestamp())

df.show(200)

df.write.mode("overwrite").option("header", True).csv(s3_output_path)
print("Archivo tratado guardado en S3.")

spark.stop()