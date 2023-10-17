import pandas as pd

# Sample data
df = pd.read_csv("extracted/10000LADYSUSDT/10000LADYSUSDT2023-05-11.csv")

# Initialize variables for the first row of each hourly interval
base_timestamp = df['timestamp'].iloc[0]
open_time = base_timestamp
low_price = high_price = df['price'].iloc[0]
volume = 0
hourly_data = []

# Helper function to create a row list with all the elements
def create_hourly_row(row, open_time, close_time, low_price, high_price, volume):
    res = [row['timestamp'], row['symbol'], row['side'], row['size'], row['price'], row['tickDirection'], row['trdMatchID'], row['grossValue'], row['homeNotional'], row['foreignNotional'], open_time, close_time, low_price, high_price, volume]
    print(res)
    return res

for index, row in df.iterrows():
    # Calculate the time difference from the base timestamp
    time_difference = row['timestamp'] - base_timestamp
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

        # Append the hourly data to the list as a single list
        hourly_data.append(create_hourly_row(row, open_time, close_time, low_price, high_price, volume))

        # Update the base timestamp to the current row's timestamp and reset openTime, low, high, and volume
        base_timestamp = row['timestamp']
        open_time = base_timestamp
        low_price = high_price = row['price']
        volume = 0


# Convert the hourly data list to a DataFrame
hourly_df = pd.DataFrame(hourly_data, columns=['timestamp', 'symbol', 'side', 'size', 'price', 'tickDirection', 'trdMatchID', 'grossValue', 'homeNotional', 'foreignNotional', 'openTime', 'closeTime', 'low', 'high', 'volume'])

# Print the hourly data
print(hourly_df)
