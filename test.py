import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
# Connection parameters
dbname = 'postgres'
user = 'postgres'
password = 'postgres'
host = 'localhost'
port = '5432'
# Establish a connection
try:
    connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    print("Connection established successfully!")
except Exception as e:
    print("Error: Unable to connect to the database:", e)
# query to fetch data
query = "SELECT * FROM xdr_data;"
cursor = connection.cursor()
cursor.execute(query)
# Fetch the top 10 rows
top_10_rows = cursor.fetchmany(size=10)

# Print the top 10 rows
print("Top 10 rows:")
for row in top_10_rows:
    print(row)