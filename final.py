import os
import pandas as pd
import psycopg2.extras
import requests
import gzip
import re
import shutil
import time

# Define your PostgreSQL connection parameters
host = "localhost"  # Replace with your PostgreSQL server's hostname or IP address
port = "5432"  # Replace with your PostgreSQL server's port
user = "prem"  # Replace with your PostgreSQL username
password = "prem"  # Replace with your PostgreSQL password
database = "exchanges_data"  # Replace with your PostgreSQL database name
table_name = "BybitTrades"  # Replace with your table name

# Create the initial PostgreSQL connection
connection = psycopg2.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    database=database
)
cursor = connection.cursor()

# Function to create or replace the table schema
def create_table(cursor):
    # Define the SQL statement to drop the existing table if it exists
    drop_table_query = f"DROP TABLE IF EXISTS {table_name}"

    # Define the SQL statement to create the new table
    create_table_query = f"""
    CREATE TABLE {table_name} (
        timestamp DOUBLE PRECISION,
        symbol VARCHAR(255),
        side VARCHAR(255),
        size DOUBLE PRECISION,
        price DOUBLE PRECISION,
        tickDirection VARCHAR(255),
        trdMatchID VARCHAR(255),
        grossValue DOUBLE PRECISION,
        homeNotional DOUBLE PRECISION,
        foreignNotional DOUBLE PRECISION,
        openTime DOUBLE PRECISION,
        closeTime DOUBLE PRECISION,
        lowPrice DOUBLE PRECISION,
        highPrice DOUBLE PRECISION,
        volumeQuote DOUBLE PRECISION
    )
    """


    try:
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
        connection.commit()
    except psycopg2.Error as e:
        print(f"Error creating or replacing the table: {e}")

# Function to insert rows into the PostgreSQL database and log
def insert_rows(data, cursor, connection):
    insert_query = f"INSERT INTO {table_name} (timestamp, symbol, side, size, price, tickDirection, trdMatchID, grossValue, homeNotional, foreignNotional, openTime, closeTime, lowPrice, highPrice, volumeQuote) VALUES %s"

    try:
        psycopg2.extras.execute_values(cursor, insert_query, data)
        connection.commit()
        print(f"Inserted {len(data)} rows.")
    except psycopg2.Error as e:
        connection.rollback()
        print(f"Error inserting data: {e}")

# Replace this with the base URL of your directory listing
base_url = "https://public.bybit.com/trading/"

# Check if the "temp" directory exists and delete it if it does
if os.path.exists("temp"):
    shutil.rmtree("temp")

# Create a "temp" directory
os.makedirs("temp")

# create_table(cursor)

# Get the page content and parse it to extract links to contracts
while True:
    try:
        response = requests.get(base_url)
        break
    except Exception as e:
        print(f"Error: {e}")
        print("Retrying in 10 seconds...")
        time.sleep(10)  # Sleep for 10 seconds before retrying

if response.status_code == 200:
    page_content = response.text

    # Use regular expressions to extract links to contracts starting with BTC, ETH, or other symbols
    contract_links = re.findall(r'<a href="((?:ETH|DAI|XRP|SOL|DOGE|BTCUSDH23|BTCUSDH24|BTCUSDM22|BTCUSDM23|BTCUSDT|BTCUSDU21|BTCUSDU22|BTCUSDU23|BTCUSDZ21|BTCUSDZ22|BTCUSDZ23)[^"]+)">', page_content)


    for contract_link in contract_links:
        contract_url = base_url + contract_link

        # Get the page content of the contract and parse it to extract links to CSV files
        while True:
            try:
                contract_response = requests.get(contract_url)
                break
            except Exception as e:
                print(f"Error: {e}")
                print("Retrying in 10 seconds...")
                time.sleep(10)  # Sleep for 10 seconds before retrying
        if contract_response.status_code == 200:
            contract_content = contract_response.text

            # Use regular expressions to extract filenames
            csv_links = re.findall(r'<a href="([^"]+)">.*?</a>', contract_content)

            for csv_link in csv_links:
                csv_url = contract_url + csv_link
                csv_file_name = os.path.basename(csv_link)
                csv_file_path = os.path.join("temp", csv_file_name)

                # Download the CSV file
                while True:
                    try:
                        response = requests.get(csv_url)
                        break
                    except Exception as e:
                        print(f"Error: {e}")
                        print("Retrying in 10 seconds...")
                        time.sleep(10)  # Sleep for 10 seconds before retrying
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

                        # Read the extracted CSV file
                        try:
                            df = pd.read_csv(extracted_file_path)
                        except Exception as e:
                            print(f"Error reading CSV file: {e}")
                            print(f"Skipping file: {extracted_file_path}")
                            continue

                        # Initialize variables for the first row of each hourly interval
                        base_timestamp = float(df['timestamp'].iloc[0])
                        open_time = float(base_timestamp)
                        low_price = float(df['price'].iloc[0])
                        high_price = low_price
                        volume = 0
                        hourly_data = []

                        def create_hourly_row(row, open_time, close_time, low_price, high_price, volume):
                            timestamp = float(row['timestamp'])  # Cast timestamp to float
                            symbol = str(row['symbol'])  # Cast symbol to string
                            side = str(row['side'])  # Cast side to string
                            size = float(row['size'])  # Cast size to float
                            price = float(row['price'])  # Cast price to float
                            tickDirection = str(row['tickDirection'])  # Cast tickDirection to string
                            trdMatchID = str(row['trdMatchID'])  # Cast trdMatchID to string
                            grossValue = float(row['grossValue'])  # Cast grossValue to float
                            homeNotional = float(row['homeNotional'])  # Cast homeNotional to float
                            foreignNotional = float(row['foreignNotional'])  # Cast foreignNotional to float

                            return (timestamp, symbol, side, size, price, tickDirection, trdMatchID, grossValue, homeNotional, foreignNotional, open_time, close_time, low_price, high_price, volume)

                        for index, row in df.iterrows():
                            # Calculate the time difference from the base timestamp
                            time_difference = abs(row['timestamp'] - base_timestamp)
                            volume += df['foreignNotional'].iloc[index]

                            # Update low and high prices for the current hour
                            if row['price'] < low_price:
                                low_price = row['price']
                            if row['price'] > high_price:
                                high_price = row['price']

                            if time_difference >= 3600:
                                # Calculate closeTime using the current row's timestamp
                                close_time = row['timestamp']
                                volume += row['foreignNotional']

                                # Append the hourly data to the list as a single tuple
                                hourly_data.append(create_hourly_row(row, open_time, close_time, low_price, high_price, volume))

                                # Update the base timestamp to the current row's timestamp and reset openTime, low, high, and volume
                                base_timestamp = row['timestamp']
                                open_time = base_timestamp
                                low_price = high_price = row['price']
                                volume = 0

                        # Insert the hourly data into the database
                        insert_rows(hourly_data, cursor, connection)

                        # Delete the extracted CSV file
                        os.remove(extracted_file_path)
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
