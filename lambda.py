import json
import sys
import boto3
import pandas as pd
from io import BytesIO


def listing_parquets_and_converting_schemas():
    s3_objects = s3.list_objects_v2(Bucket=env['bucket'],Prefix=f"specialized/{env['table_name']}/")

    for item in s3_objects['Contents']:
        df = read_s3_files_into_dataframe(env['bucket'],item['Key'])
        converted_df = df.astype(env['schema'])
        put_parquet(converted_df, env['bucket'], item['Key'])

def read_s3_files_into_dataframe(bucket_name, path):
    logger.info(f"Reading s3 file from {bucket_name}, path {path}")
    response = s3.get_object(Bucket=bucket_name,Key=path)
    body = BytesIO(response['Body'].read())
    df = pd.read_parquet(body)
    return df
    
def put_parquet(converted_df,bucket_name,path):
    converted_parquet = BytesIO(converted_df.to_parquet(compression='snappy'))
    s3.put_object(Bucket=bucket_name,Key=path,Body=converted_parquet)
    logger.info(f"Uploaded converted parquet into {bucket_name}, path {path}")


class Logger:
    def info(str):
        print(str)
        

def lambda_handler(event, context):
    logger = Logger()
    logger.info(f"***INITIALIZING GLUEJOB SCRIPT***     Parameters: Bucket = {env['bucket']}, Schema = {env['schema']}, Table name = {env['table_name']}")
    
    s3 = boto3.client('s3')
    
    listing_parquets_and_converting_schemas()
    
    logger.info("***FINISHED GLUEJOB SCRIPT***")
