import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import boto3

def main(table_name, column_schema):
    father_path = f"{table_name}"  #Substitute for new path when uploading to AWS

    for folder in bucket:
        pass

    paths = []

    iterate_through_all_files(father_path)

    if not paths:
        logger.info(f"No parquet file found in: {father_path}")

    for path in paths:
        original_df = read_parquet(path)
        logger.info(f"\nOriginal df{paths.index(path)}: \n{original_df.dtypes}")
        new_df = original_df.astype(column_schema)
        logger.info(f"\n Modified df{paths.index(path)}: \n{new_df.dtypes}")

def read_parquet(path):
    #Read Parquet and return dataframe pandas
    return pd.read_parquet(path)

def iterate_through_all_files(path):
    for file in os.scandir(path):
        if file.is_file():
            if os.path.splitext(file)[1] == ".parquet":
                logger.info("Parquet file found in: " + file.path)
                paths.append(file.path)
        else:
            iterate_through_all_files(file.path)


## @params: [JOB_NAME]
env = getResolvedOptions(sys.argv, ['JOB_NAME', 'table', 'schema'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(env['JOB_NAME'], env)  #Start gluejob

s3 = boto3.resource('s3')
bucket = s3.Bucket('specialized/')

logger = glueContext.get_logger()

main(env["table"],env["schema"])

job.commit()  #Finish gluejob