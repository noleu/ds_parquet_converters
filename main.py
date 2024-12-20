# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pandas as pd

def convert_to_parquett(name):
    # Use a breakpoint in the code line below to debug your script.

    # Specify the input Parquet file and the output CSV file
    parquet_file = f"./{name}.parquet"  # Replace with your Parquet file path
    csv_file = f"./{name}.csv"  # Replace with your desired CSV file path

    # Load the Parquet file into a DataFrame
    df = pd.read_parquet(parquet_file, engine="pyarrow")  # Use engine="fastparquet" if needed

    print(type(df['price'][0]))
    df['price'] = 1000.1
    print(df['price'][0])
    print(type(df['price'][0]))
    # Save the DataFrame to a CSV file
    df.to_csv(csv_file, index=False)

    print(f"CSV file created: {csv_file}")


    # Save DataFrame to Parquet format
    df.to_parquet(f"./{name}-2.parquet", engine="pyarrow", index=False)  # Use engine="fastparquet" if preferred

    print(f"Parquet file created: {parquet_file}")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    convert_to_parquett('pricing/c7g.2xlarge')
    convert_to_parquett('pricing/m7g.medium')
    convert_to_parquett('pricing/m7g.xlarge')
    convert_to_parquett('pricing/r7g.2xlarge')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
