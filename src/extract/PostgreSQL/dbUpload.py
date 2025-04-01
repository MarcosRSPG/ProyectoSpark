from pyspark.sql import SparkSession

aws_access_key_id='test'
aws_secret_access_key='test'

def read_from_postgres():
    spark = SparkSession.builder \
        .appName("ReadFromPostgres") \
        .config("spark.jars", "postgresql-42.7.3.jar") \
        .master("local[*]") \
        .getOrCreate()

    # Define connection properties
    jdbc_url = "jdbc:postgresql://postgres-db:5432/retail_db"
    connection_properties = {
        "user": "postgres",
        "password": "casa1234",
        "driver": "org.postgresql.Driver"
    }

    table_name = "Stores"
    try:
        # Read data from PostgreSQL table into a DataFrame
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

        df.write.mode("overwrite").option("header", True).csv("../Spark/data/stores.csv")

    except Exception as e:
        print("Error reading data from PostgreSQL:", e)
    finally:
        spark.stop()

if __name__ == "__main__":
    read_from_postgres()