CREATE TABLE csv_data (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    store_id INT,
    product_id VARCHAR(10),
    quantity_sold FLOAT,
    revenue FLOAT,
    tratado BOOLEAN,
    fecha_insercion TIMESTAMP
);

CREATE TABLE kafka_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    store_id INT,
    product_id VARCHAR(10),
    quantity_sold FLOAT,
    revenue FLOAT,
    tratado BOOLEAN,
    fecha_insercion TIMESTAMP
);

CREATE TABLE db_data (
    store_id INT PRIMARY KEY,
    name VARCHAR(10),
    location VARCHAR(255),
    demographic VARCHAR(255),
    tratado BOOLEAN,
    fecha_insercion TIMESTAMP
);