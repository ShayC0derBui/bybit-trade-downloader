import os
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Define your MySQL connection parameters
host = "tonnochycapital.com"
user = "prem"
password = "jr5NkkkjKK&M@f5HtgL&5HYN9bSt!fd@Vz9*#cg@3Hmdt#sfhyRGyTQ2C%xV73zp"
database = "exchanges_data"
table_name = "BybitTrades"  # Replace with your table name

# Function to create or replace the table schema
def create_table(cursor):
    # Define the SQL statement to drop the existing table if it exists
    drop_table_query = "DROP TABLE IF EXISTS {}".format(table_name)

    # Define the SQL statement to create the new table
    create_table_query = """
    CREATE TABLE {} (
        timestamp DOUBLE,
        symbol VARCHAR(255),
        side VARCHAR(255),
        size INT,
        price DOUBLE,
        tickDirection VARCHAR(255),
        trdMatchID VARCHAR(255),
        grossValue DOUBLE,
        homeNotional DOUBLE,
        foreignNotional DOUBLE
    )
    """.format(table_name)

    try:
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
    except Error as e:
        print(f"Error creating or replacing the table: {e}")

# Function to insert rows into the MySQL database and log
def insert_rows(data, cursor, connection):
    insert_query = "INSERT INTO {} (timestamp, symbol, side, size, price, tickDirection, trdMatchID, grossValue, homeNotional, foreignNotional) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_name)

    try:
        cursor.executemany(insert_query, data)
        print(f"Inserted {len(data)} rows.")
        connection.commit()
    except Error as e:
        connection.rollback()
        print(f"Error inserting data: {e}")

# Path to the directory containing CSV files
csv_dir = "extracted"

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

        # Create or replace the table schema
        create_table(cursor)

        # Traverse the directory structure
        for folder_name, subfolders, files in os.walk(csv_dir):
            for filename in files:
                if filename.endswith(".csv"):
                    csv_file = os.path.join(folder_name, filename)
                    df = pd.read_csv(csv_file)

                    # Convert DataFrame to a list of tuples
                    data = [tuple(row) for row in df.values]

                    # Insert rows into the database and commit after every 500 rows
                    batch_size = 500
                    for i in range(0, len(data), batch_size):
                        insert_rows(data[i:i+batch_size], cursor, connection)

        # Commit any remaining rows
        connection.commit()

        cursor.close()
except Error as e:
    print(f"Error: {e}")
finally:
    if connection.is_connected():
        connection.close()
