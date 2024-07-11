import pandas as pd
import numpy as np
import os


def main(table_name, column_schema):
    father_path = f"C:/Users/t.david.hamoui/parquet-schema-data-converter/s3/" + table_name

    global paths
    paths = []

    iterate_through_all_files(father_path)

    if not paths:
        print("No parquet file found in: " + father_path)

    for path in paths:
        original_df_customer_id = read_parquet(path)
        new_df = original_df_customer_id.astype(column_schema)
        print(new_df)

def read_parquet(path):
    #Read Parquet and return dataframe pandas
    return pd.read_parquet(path)

def iterate_through_all_files(path):

    for file in os.scandir(path):
        if file.is_file():
            if os.path.splitext(file)[1] == ".parquet":
                print("Parquet file found in: " + file.path)
                paths.append(file.path)
        else:
            iterate_through_all_files(file.path)


parameter_dict = {
    "customer_id": int
}

table_name = 'retail_orders'

main(table_name=table_name,column_schema=parameter_dict)