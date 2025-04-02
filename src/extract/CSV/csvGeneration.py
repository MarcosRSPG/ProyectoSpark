from faker import Faker
import random as rdm

csvRute= 'Spark/data/sales_data.csv'
headers = 'date, store_ID, product_ID, quantity_Sold, revenue \n'
content= ''

fk= Faker()

fileCSV = open(csvRute, 'w+')
fileCSV.write(headers)

def dataGenerator(type):
    probability =rdm.random()
    match(type):
        case 'date': data = fk.date_this_year()
        case 'store': data = rdm.randint(1, 100)
        case 'product': data = fk.bothify(text='???-###').upper()
        case 'quantity': data = rdm.randint(1, 50)
        case 'revenue': data = round(rdm.uniform(10, 1000), 2)
    if 0 <= probability < 0.05:
        data = None
    if 0.05 <= probability < 0.075:
        data = ''
    if 0.075 <= probability < 0.125:
        data = f'{type}_error'.upper()
    return data

for i in range(1, 5):
    content += f'{dataGenerator('date')}, {dataGenerator('store')}, {dataGenerator('product')}, {dataGenerator('quantity')}, {dataGenerator('revenue')} \n'

fileCSV.write(content)

fileCSV.close()