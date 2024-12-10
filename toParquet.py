from pyarrow import json
import pyarrow.parquet as pq
import pandas as pd
import json

files = ["m7g_medium_spot_pricing_cleaned", "m7g_xlarge_spot_pricing_cleaned", "c7g_2xlarge_spot_pricing_cleaned", "r7g_2xlarge_spot_pricing_cleaned"]

for file in files:
    split = file.split('_')
    new_name = split[0] + '.' + split[1]
    # Read the JSON file
    with open(f'pricing/{file}.json', 'r') as file:
        data = json.load(file)

    # Extract the spotPrices array
    spot_prices = data['spotPrices']

    # Convert the array to a DataFrame
    df = pd.DataFrame(spot_prices)
    df.rename(columns={'price': 'spot_price'}, inplace=True)

    # Convert timestamp to Unix epoch time in milliseconds
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['timestamp'] = df['timestamp'].astype('int64') // 10 ** 6

    # Calculate 80% of the maximum price
    max_price = df['spot_price'].max()
    on_demand_price = 0.8 * max_price

    # Add the new column
    df['on_demand_price'] = on_demand_price

    # Save the DataFrame as a Parquet file
    df.to_parquet(f'pricing/{new_name}.parquet')
    print(f'Saved {new_name}.parquet')