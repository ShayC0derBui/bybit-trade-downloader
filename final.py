import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import requests
import gzip
import re
import shutil

# Define your MySQL connection parameters
host = "tonnochycapital.com"
user = "prem"
password = "jr5NkkkjKK&M@f5HtgL&5HYN9bSt!fd@Vz9*#cg@3Hmdt#sfhyRGyTQ2C%xV73zp"
database = "exchanges_data"
table_name = "BybitTrades"  # Replace with your table name

# Create the initial MySQL connection
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
cursor = connection.cursor()

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

# Replace this with the base URL of your directory listing
base_url = "https://public.bybit.com/trading/"

# Check if the "temp" directory exists and delete it if it does
if os.path.exists("temp"):
    shutil.rmtree("temp")

# Create a "temp" directory
os.makedirs("temp")

# Get the page content and parse it to extract links to contracts
response = requests.get(base_url)
if response.status_code == 200:
    page_content = response.text

    # Use regular expressions to extract links to contracts
    contract_links = re.findall(r'<a href="([^"]+/)">.*?</a>', page_content)

    for contract_link in contract_links:
        contract_url = base_url + contract_link

        # Get the page content of the contract and parse it to extract links to CSV files
        contract_response = requests.get(contract_url)
        if contract_response.status_code == 200:
            contract_content = contract_response.text

            # Use regular expressions to extract filenames
            csv_links = re.findall(r'<a href="([^"]+)">.*?</a>', contract_content)

            for csv_link in csv_links:
                csv_url = contract_url + csv_link
                csv_file_name = os.path.basename(csv_link)
                csv_file_path = os.path.join("temp", csv_file_name)

                # Download the CSV file
                response = requests.get(csv_url)
                if response.status_code == 200:
                    with open(csv_file_path, 'wb') as csv_file:
                        csv_file.write(response.content)

                    # Check if the downloaded file is a GZIP file and extract it using gzip library
                    if csv_file_name.endswith('.gz'):
                        extracted_file_path = os.path.join("temp", csv_file_name[:-3])
                        with open(csv_file_path, 'rb') as f_in, open(extracted_file_path, 'wb') as f_out:
                            with gzip.open(f_in, 'rb') as g_in:
                                f_out.write(g_in.read())
                        print(f"Downloaded and extracted: {extracted_file_path}")
                        os.remove(csv_file_path)  # Delete the downloaded CSV file

                        # Insert the extracted data into the database
                        df = pd.read_csv(extracted_file_path)
                        data = [tuple(row) for row in df.values]

                        try:
                            # Insert the CSV data into the database
                            batch_size = 200000
                            for i in range(0, len(data), batch_size):
                                insert_rows(data[i:i + batch_size], cursor, connection)
                            os.remove(extracted_file_path)  # Delete the downloaded and extracted CSV file
                        except Error as e:
                            print(f"Error inserting data: {e}")
                            # Reconnect to the MySQL server and retry the insert
                            connection.reconnect()
                            cursor = connection.cursor()
                            batch_size = 200000
                            for i in range(0, len(data), batch_size):
                                insert_rows(data[i:i + batch_size], cursor, connection)
                            os.remove(extracted_file_path)  # Delete the downloaded and extracted CSV file

                    else:
                        print(f"Downloaded: {csv_file_path}")

                else:
                    print(f"Failed to download: {csv_link}")

    # Close the cursor and connection after all CSVs are processed
    cursor.close()
    connection.close()

    # Recursively delete the "temp" directory and its contents
    shutil.rmtree("temp")

else:
    print("Failed to access the directory listing.")
