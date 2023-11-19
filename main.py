import os
import pandas as pd
import psycopg2.extras
import requests
import gzip
import re
import shutil
import time
from datetime import datetime, timedelta

# Define your PostgreSQL connection parameters
host = "localhost"  # Replace with your PostgreSQL server's hostname or IP address
port = "5432"  # Replace with your PostgreSQL server's port
user = "prem"  # Replace with your PostgreSQL username
password = "prem"  # Replace with your PostgreSQL password
database = "exchanges_data"  # Replace with your PostgreSQL database name
table_name = "Trades"  # Replace with your table name

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

    extension_query = "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""

    # Define the SQL statement to create the new table
    create_table_query = f"""
    CREATE TABLE {table_name} (
        id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
        timestamp BIGINT,
        symbol VARCHAR(255),
        side VARCHAR(255),
        sizeQuote DOUBLE PRECISION,
        openTime BIGINT,
        closeTime BIGINT,
        lowPrice DOUBLE PRECISION,
        highPrice DOUBLE PRECISION,
        openPrice DOUBLE PRECISION,
        closePrice DOUBLE PRECISION,
        volumeQuote DOUBLE PRECISION,
        market VARCHAR(255),
        exchange VARCHAR(255)
    )
    """


    try:
        cursor.execute(drop_table_query)
        cursor.execute(extension_query)
        cursor.execute(create_table_query)
        connection.commit()
    except psycopg2.Error as e:
        print(f"Error creating or replacing the table: {e}")

create_table(cursor)

# Function to insert rows into the PostgreSQL database and log
def insert_rows(data, cursor, connection):
    insert_query = f"INSERT INTO {table_name} (timestamp, symbol, side, sizeQuote, openTime, closeTime, lowPrice, highPrice, openPrice, closePrice, volumeQuote, market, exchange) VALUES %s"

    try:
        psycopg2.extras.execute_values(cursor, insert_query, data)
        connection.commit()
        print(f"Inserted {len(data)} rows.")
    except psycopg2.Error as e:
        connection.rollback()
        print(f"Error inserting data: {e}")

# Replace this with the base URL of your directory listing
url = {'base_url_future': 'https://public.bybit.com/trading/', 'base_url_spot': 'https://public.bybit.com/spot/'}

# Check if the "temp" directory exists and delete it if it does
if os.path.exists("temp"):
    shutil.rmtree("temp")

# Create a "temp" directory
os.makedirs("temp")

for market,base_url in url.items():
    if market == "base_url_future":
        future = True
    elif market == "base_url_spot":
        future = False

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
        contract_links = re.findall(r'<a href="((?:ADA|BTC|ETH|DAI|XRP|SOL|DOGE)[^"]+)">', page_content)


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

                # Set your desired start_time
                start_time = None

                # Initialize variables
                this_is_start_row = True
                open_time = None
                open_price = 0
                low_price = float('inf')
                high_price = 0
                volume = 0
                hourly_data = []
                is_it_the_first_time  = True

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

                            def create_hourly_row(row, open_time, close_time, low_price, high_price, open_price, close_price, volume):
                                timestamp = float(row['timestamp'])  # Cast timestamp to float
                                symbol = str(row['symbol']).upper()  # Cast symbol to string
                                side = str(row['side']).upper()  # Cast side to string
                                size = float(row['size'])  # Cast size to float
                                exchange = "BYBIT"
                                if not future:
                                    market = "SPOT"
                                else:
                                    if symbol.endswith("USDT") or symbol.endswith("PERP") or "-" in symbol:
                                        market = "LINEAR"
                                    elif symbol.endswith("USD") or re.match(r"[A-Za-z][A-Za-z][A-Za-z]USD[A-Za-z]\d\d", symbol):
                                        market = "INVERSE"

                                return (timestamp, symbol, side, size, open_time, close_time, low_price, high_price, open_price, close_price, volume, market, exchange)

                            
                            if is_it_the_first_time:
                                cur_time = datetime.fromtimestamp(df['timestamp'].iloc[0])
                                
                                # Find the closest next hour to cur_time and set it as start time
                                start_time = cur_time + timedelta(hours=1) - timedelta(minutes=cur_time.minute, seconds=cur_time.second)

                                is_it_the_first_time = False
                            for index, row in df.iterrows():
                                trade_time = datetime.fromtimestamp(row['timestamp'])
                                # Check if we haven't reached start_time, continue to the next loop iteration
                                if trade_time < start_time:
                                    continue
                                else:
                                    if this_is_start_row:
                                        this_is_start_row = False
                                        open_time = start_time
                                        open_price = row['price']
                                        volume += row['homeNotional']
                                        low_price = min(low_price, row['price'])
                                        high_price = max(high_price, row['price'])
                                    else:
                                        if trade_time < start_time + timedelta(hours=1):
                                            volume += row['homeNotional']
                                            low_price = min(low_price, row['price'])
                                            high_price = max(high_price, row['price'])
                                        else:
                                            close_price = df['price'].iloc[index-1]
                                            close_time = start_time + timedelta(hours=1)
                                            hourly_data.append(create_hourly_row(df.iloc[index-1], datetime.timestamp(open_time), datetime.timestamp(close_time),low_price, high_price, open_price, close_price, volume))

                                            # Reset variables for the next hour
                                            start_time = close_time
                                            this_is_start_row = True
                                            open_time = start_time
                                            open_price = row['price']
                                            low_price = float('inf')
                                            high_price = 0
                                            volume = row['homeNotional']

                            # Insert the hourly data into the database
                            insert_rows(hourly_data, cursor, connection)
                            hourly_data = []

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
