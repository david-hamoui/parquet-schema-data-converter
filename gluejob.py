import pandas as pd
import numpy as np
import fastparquet
import pyarrow


def main(table_name, column_schema):
    path = f"C:/Users/t.david.hamoui/parquet-schema-data-converter/s3/retail_orders/partition2/retail_orders.parquet"
    
    original_df = read_parquet(path, column_schema)

    new_df = original_df.apply(convert_string_to_upper, axis=0)

    print(new_df)

def convert_string_to_upper(str):
    pass

def read_parquet(table_name, column):
    #Read Parquet and return dataframe pandas
    return pd.read_parquet(table_name, columns=column)


main(table_name='',column_schema=['customer_id'])