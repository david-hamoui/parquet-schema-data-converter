import pandas as pd
import numpy as np
import os


def main(table_name, column_schema):
    father_path = f"{os.getcwd()}/s3/{table_name}"  #Substitute for new path when uploading to AWS

    global paths
    paths = []

    iterate_through_all_files(father_path)

    if not paths:
        Logger.info(f"No parquet file found in: {father_path}")

    for path in paths:
        original_df = read_parquet(path)
        Logger.info(f"\nOriginal df{paths.index(path)}: \n{original_df.dtypes}")
        new_df = original_df.astype(column_schema)
        Logger.info(f"\n Modified df{paths.index(path)}: \n{new_df.dtypes}")

def read_parquet(path):
    #Read Parquet and return dataframe pandas
    return pd.read_parquet(path)

def iterate_through_all_files(path):
    for file in os.scandir(path):
        if file.is_file():
            if os.path.splitext(file)[1] == ".parquet":
                Logger.info("Parquet file found in: " + file.path)
                paths.append(file.path)
        else:
            iterate_through_all_files(file.path)


class Logger:
    def info(str):
        print(str)


parameter_dict = {
    "id": "string[python]",
    "customer_id": "string[python]",
    "agreement": "string[python]",
    "status": "string[python]",
    "checkout_order_id": "string[python]",
    "charge_id": "string[python]",
    "number": "string[python]",
    "checkout_order_xml": "string[python]",
    "created_at": "object",
    "updated_at": "string[python]",
    "generic_attributes": "string[python]",
    "antifraud_id": "string[python]"
}

table_name = 'retail_orders'

env = {"table": table_name, "schema": parameter_dict}

main(env["table"],env["schema"])