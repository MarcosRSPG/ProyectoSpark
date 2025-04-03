from pyspark.sql import SparkSession

aws_access_key_id='test'
aws_secret_access_key='test'

def read_from_postgres():
    spark = SparkSession.builder \
            .appName("UPLOAD DE POSGRE A S3") \
            .config("spark.driver.extraClassPath", "/opt/spark-apps/drivers/postgresql-42.7.3.jar:/opt/spark/jars/*") \
            .config("spark.executor.extraClassPath", "/opt/spark-apps/drivers/postgresql-42.7.3.jar:/opt/spark/jars/*") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
            .config("spark.hadoop.fs.s3a.access.key", "test") \
            .config("spark.hadoop.fs.s3a.secret.key", "test") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .master("spark://spark-master:7077") \
            .getOrCreate()

    # Define connection properties
    jdbc_url = "jdbc:postgresql://postgres-db:5432/retail_db"
    connection_properties = {
        "user": "postgres",
        "password": "casa1234",
        "driver": "org.postgresql.Driver"
    }

    table_name = "stores"
    try:
        # Read data from PostgreSQL table into a DataFrame
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

        df \
            .write \
            .option('header', 'true') \
            .option('fs.s3a.committer.name', 'partitioned') \
            .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
            .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
            .mode('overwrite') \
            .csv(path='s3a://data-lake/postgres/', sep=',')

    except Exception as e:
        print("Error reading data from PostgreSQL:", e)
    finally:
        spark.stop()

if __name__ == "__main__":
    read_from_postgres()