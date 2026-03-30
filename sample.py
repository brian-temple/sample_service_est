# sample_service_est

# AWS Lambda-based system to collect fuel prices + material costs
# and generate estimates using PostgreSQL joins

import json import os import psycopg2 import requests from datetime import datetime

DB_HOST = os.environ.get("DB_HOST") DB_NAME = os.environ.get("DB_NAME") DB_USER = os.environ.get("DB_USER") DB_PASS = os.environ.get("DB_PASS")

# DATABASE CONNECTION
def get_connection(): return psycopg2.connect( host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS )

# FETCH FUEL PRICES (API MOCK)
def fetch_fuel_prices(): # Replace with real API (e.g., EIA, GasBuddy API) return [ {"type": "gas", "price": 3.45, "location": "NC"}, {"type": "diesel", "price": 3.95, "location": "NC"} ]

# FETCH MATERIAL COSTS (MOCK)
def fetch_material_costs(): # Replace with scraping or APIs (Home Depot, Lowe's, etc.) return [ {"material": "lumber", "price": 8.50}, {"material": "concrete", "price": 120.00}, {"material": "drywall", "price": 15.75} ]

# STORE DATA
def store_data(): conn = get_connection() cur = conn.cursor()

fuel_data = fetch_fuel_prices()
material_data = fetch_material_costs()

for f in fuel_data:
    cur.execute("""
        INSERT INTO fuel_prices (type, price, location, created_at)
        VALUES (%s, %s, %s, %s)
    """, (f["type"], f["price"], f["location"], datetime.utcnow()))

for m in material_data:
    cur.execute("""
        INSERT INTO material_costs (material, price, created_at)
        VALUES (%s, %s, %s)
    """, (m["material"], m["price"], datetime.utcnow()))

conn.commit()
cur.close()
conn.close()

# ESTIMATION USING JOIN
def generate_estimate(): conn = get_connection() cur = conn.cursor()

query = """
    SELECT 
        m.material,
        m.price AS material_price,
        f.type AS fuel_type,
        f.price AS fuel_price,
        (m.price * 10 + f.price * 5) AS estimated_cost
    FROM material_costs m
    JOIN fuel_prices f
    ON f.location = 'NC'
    ORDER BY m.material;
"""

cur.execute(query)
results = cur.fetchall()

estimates = []
for row in results:
    estimates.append({
        "material": row[0],
        "material_price": float(row[1]),
        "fuel_type": row[2],
        "fuel_price": float(row[3]),
        "estimated_cost": float(row[4])
    })

cur.close()
conn.close()

return estimates

# LAMBDA HANDLER
def lambda_handler(event, context): action = event.get("action", "collect")

if action == "collect":
    store_data()
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Data collected successfully"})
    }

elif action == "estimate":
    estimates = generate_estimate()
    return {
        "statusCode": 200,
        "body": json.dumps(estimates)
    }

else:
    return {
        "statusCode": 400,
        "body": json.dumps({"error": "Invalid action"})
    }

# SQL SCHEMA (RUN SEPARATELY)
""" CREATE TABLE fuel_prices ( id SERIAL PRIMARY KEY, type VARCHAR(10), price NUMERIC, location VARCHAR(10), created_at TIMESTAMP );

CREATE TABLE material_costs ( id SERIAL PRIMARY KEY, material VARCHAR(50), price NUMERIC, created_at TIMESTAMP ); """
