import psycopg2
import pandas as pd

# Configuración de la conexión a PostgreSQL
db_config = {
    "host": "localhost",  # O la IP de tu servidor PostgreSQL
    "database": "retail_db",
    "user": "postgres",
    "password": "casa1234",
    "port": "5432"
}

# Nombre de la tabla
table_name = "Stores"

# Ruta local donde guardar el CSV
csv_path = "C:/Users/34652/Documents/ProyectoSpark/Spark/data/stores.csv"  # Cambia la ruta si es necesario

try:
    # Conectar a PostgreSQL
    conn = psycopg2.connect(**db_config)
    
    # Crear un DataFrame con los datos de la tabla
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, conn)

    # Guardar el DataFrame en un archivo CSV en local
    df.to_csv(csv_path, index=False)

except Exception as e:
    print(f"Error al leer la base de datos: {e}")

finally:
    if conn:
        conn.close()  # Cerrar la conexión
