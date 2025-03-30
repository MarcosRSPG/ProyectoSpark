import psycopg2
import random as rdm
from faker import Faker
from dotenv import load_dotenv
import os

load_dotenv('./config/.env')

fk = Faker()

def dataGenerator(type):
    probability =rdm.random()
    match(type):
        case 'store': data = fk.company()
        case 'location': data = fk.street_address()
        case 'demographics': data = f'({fk.latitude()},{fk.longitude()})'
    if 0 <= probability < 0.05:
        data = None
    if 0.05 <= probability < 0.075:
        data = ''
    if 0.075 <= probability < 0.125:
        data = f'{type}_error'.upper()
    return data

try:
    conn = psycopg2.connect(
        host= os.getenv('HOST'),
        database= os.getenv('DATABASE'),
        user= os.getenv('USER'),
        password= os.getenv('PASSWORD'),
        port= os.getenv('PORT')
    )
    createTableString='CREATE TABLE IF NOT EXISTS Stores (store_id SERIAL PRIMARY KEY, store_name VARCHAR(255) NOT NULL, location VARCHAR(255) NOT NULL, demographics VARCHAR(255) NOT NULL);'
    with conn.cursor() as cur:
        cur.execute(createTableString)
    conn.commit()
        

    with conn.cursor() as cur:
        for x in range(5000):
            insertString=f'INSERT INTO Stores  (store_name,location,demographics) VALUES (\'{dataGenerator('store')}\',\'{dataGenerator('location')}\',\'{dataGenerator('demographics')}\')'
            cur.execute(insertString)
        conn.commit()

                
except (psycopg2.DatabaseError, Exception) as error:
    print(error)