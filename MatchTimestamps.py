# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pandas as pd
import parquet_file_creator as pfc


def match_time_stamps(workload_path, price_path):
    # Use a breakpoint in the code line below to debug your script.

    # Specify the input Parquet file and the output CSV file

    # Load the Parquet file into a DataFrame
    df_price = pd.read_parquet(price_path, engine="pyarrow")  # Use engine="fastparquet" if needed

    df_price['timestamp'] = pd.to_datetime(df_price['timestamp'], unit='ms')
    lowest_timestamp_price = pd.to_datetime(df_price['timestamp'].min())
    highest_timestamp_price = pd.to_datetime(df_price['timestamp'].max())

    df_workload = pd.read_parquet(workload_path, engine="pyarrow")  # Use engine="fastparquet" if needed

    original_timestamps = pd.to_datetime(df_workload['submission_time'])
    original_frequency = (original_timestamps.max() - original_timestamps.min()) / (len(original_timestamps) - 1)


    # Generate new timestamps within the specified range
    new_timestamps = pd.date_range(start=lowest_timestamp_price, end=highest_timestamp_price, freq=original_frequency)[:len(df_workload)]
    df_workload['submission_time'] = new_timestamps
    df_workload['submission_time'] = df_workload['submission_time'].astype('int64') // 10**6


    if not "snapshot_delay" in df_workload.columns:
        df_workload['snapshot_delay'] = 0 * df_workload['duration']

    # Save DataFrame to Parquet format
    # new_workload_path = workload_path.replace('.parquet', '-new.parquet')
    # df_workload.to_parquet(new_workload_path, engine="pyarrow", index=False)  # Use engine="fastparquet" if preferred

    pfc.writeTrace(df_workload, './aws-bitbrains-small')

    # print(f"Parquet file created: {new_workload_path}")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    match_time_stamps( './bitbrains-small/tasks.parquet', './pricing/c7g.2xlarge.parquet')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
