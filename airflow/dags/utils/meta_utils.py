from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import io

def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='logicat_snowflake_conn')
    return hook.get_conn().cursor()

@task
def read_field_csv(s3_bucket, s3_key, aws_conn_id):
    hook = S3Hook(aws_conn_id=aws_conn_id)
    csv_content = hook.read_key(s3_key, bucket_name=s3_bucket)
    df = pd.read_csv(io.StringIO(csv_content))
    return df