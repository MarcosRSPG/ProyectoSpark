from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker
import random as rdm

fk = Faker()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

def dataGenerator(type):
    probability = rdm.random()
    match(type):
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

for i in range(1, 20):
    message = {
        "timestamp": int(datetime.now().timestamp() * 1000),
        "store_id": dataGenerator('store'),
        "product_id": dataGenerator('product'),  
        "quantity_sold": dataGenerator('quantity'),  
        "revenue": dataGenerator('revenue')
    }
    print(i)
    producer.send('sales_stream', value=message)
    sleep(1)

