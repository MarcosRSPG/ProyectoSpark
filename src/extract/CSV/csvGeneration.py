from faker import Faker
import random as rdm
import os

csvRute= './src/extract/CSV/data/sales_data.csv'
headers = 'Date, Store ID, Product ID, Quantity Sold, Revenue'
contenido= ''

fk= Faker()

fileCSV = open(csvRute, 'a+')
fileCSV.write(headers)

for i in range(1, 5001):
    date= fk.date()
    storeId= rdm.randint(1, 1000)
    productId= rdm.randint(1, 1000)
    quantitySold= rdm.randint(1, 80000)
    revenue= rdm.randint(1,100000)
    contenido += f'{date}, {storeId}, {productId}, {quantitySold}, {revenue} \n'

fileCSV.write(contenido)

fileCSV.close()