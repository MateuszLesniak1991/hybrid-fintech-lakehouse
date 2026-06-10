CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    risk_score INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    account_type VARCHAR(50),
    currency VARCHAR(10),
    status VARCHAR(50),
    opened_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id INTEGER PRIMARY KEY,
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100)
);
