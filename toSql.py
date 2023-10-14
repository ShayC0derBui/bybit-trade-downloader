import os
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Define your MySQL connection parameters
host = "tonnochycapital.com"
user = "prem"
password = "jr5NkkkjKK&M@f5HtgL&5HYN9bSt!fd@Vz9*#cg@3Hmdt#sfhyRGyTQ2C%xV73zp"
database = "exchanges_data"

# Function to insert rows into the MySQL database
def insert_rows(data, cursor, connection):
    insert_query = "INSERT INTO your_table_name (timestamp, symbol, side, size, price, tickDirection, trdMatchID, grossValue, homeNotional, foreignNotional) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    try:
        cursor.executemany(insert_query, data)
        connection.commit()
        print(f"Inserted {len(data)} rows.")
    except Error as e:
        connection.rollback()
        print(f"Error: {e}")

# Path to the directory containing CSV files
csv_dir = "path_to_directory/extracted"

# Connect to the MySQL database
try:
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    if connection.is_connected():
        cursor = connection.cursor()

        # Iterate through CSV files in the directory
        for filename in os.listdir(csv_dir):
            if filename.endswith(".csv"):
                csv_file = os.path.join(csv_dir, filename)
                df = pd.read_csv(csv_file)

                # Convert DataFrame to a list of tuples
                data = [tuple(row) for row in df.values]

                # Insert rows into the database and commit after every 500 rows
                batch_size = 500
                for i in range(0, len(data), batch_size):
                    insert_rows(data[i:i+batch_size], cursor, connection)

        cursor.close()
except Error as e:
    print(f"Error: {e}")
finally:
    if connection.is_connected():
        connection.close()
