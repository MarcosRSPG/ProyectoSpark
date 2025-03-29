from faker import Faker
import random as rdm

csvRute= './src/extract/CSV/data/sales_data.csv'
headers = 'Date, Store ID, Product ID, Quantity Sold, Revenue'
contenido= ''

fk= Faker()

fileCSV = open(csvRute, 'w+')
fileCSV.write(headers)

def date_gen():
    probabilidad =rdm.random()
    date = fk.date_this_year()
    if 0 <= probabilidad < 0.05:
        date = None
    if 0.05 <= probabilidad < 0.075:
        date = ''
    if 0.075 <= probabilidad < 0.125:
        date = 'date_error'
    return date


def store_id_gen():
    probabilidad =rdm.random()
    store_id = rdm.randint(1, 100)
    if 0 <= probabilidad < 0.05:
        store_id = None
    if 0.05 <= probabilidad < 0.075:
        store_id = ''
    if 0.075 <= probabilidad < 0.125:
        store_id = 'store_error'
    return store_id


def product_id_gen():
    probabilidad =rdm.random()
    product_id = fk.bothify(text='???-###').upper()
    if 0 <= probabilidad < 0.05:
        product_id = None
    if 0.05 <= probabilidad < 0.075:
        product_id = ''
    if 0.075 <= probabilidad < 0.125:
        product_id = 'product_error'
    return product_id


def quantity_sold_gen():
    probabilidad =rdm.random()
    quantity_sold = rdm.randint(1, 50)
    if 0 <= probabilidad < 0.05:
        quantity_sold = None
    if 0.05 <= probabilidad < 0.075:
        quantity_sold = ''
    if 0.075 <= probabilidad < 0.125:
        quantity_sold = 'quantity_error'
    return quantity_sold


def revenue_gen():
    probabilidad =rdm.random()
    revenue = round(rdm.uniform(10, 1000), 2)
    if 0 <= probabilidad < 0.05:
        revenue = None
    if 0.05 <= probabilidad < 0.075:
        revenue = ''
    if 0.075 <= probabilidad < 0.125:
        revenue = 'revenue_error'
    return revenue


for i in range(1, 5001):
    contenido += f'{date_gen()}, {store_id_gen()}, {product_id_gen()}, {quantity_sold_gen()}, {revenue_gen()} \n'

fileCSV.write(contenido)

fileCSV.close()