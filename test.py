import pandas as pd

# Sample data
df= pd.read_csv("extracted/10000LADYSUSDT/10000LADYSUSDT2023-05-11.csv")

# Initialize the base timestamp as the first row's timestamp
base_timestamp = df['timestamp'].iloc[0]

# List to store hourly data
hourly_data = []

for index, row in df.iterrows():
    # Calculate the time difference from the base timestamp
    time_difference = row['timestamp'] - base_timestamp

    # Check if the time difference is greater than or equal to 3600 seconds (1 hour)
    if time_difference >= 3600:
        hourly_data.append(row)
        print(row)
        # Update the base timestamp to the current row's timestamp
        base_timestamp = row['timestamp']

# Convert the hourly data back to a DataFrame
hourly_df = pd.DataFrame(hourly_data)

# Reset the index of the new DataFrame
hourly_df = hourly_df.reset_index(drop=True)

# Print the hourly data
print(hourly_df)
