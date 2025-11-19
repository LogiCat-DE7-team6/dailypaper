
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow import DAG

from scripts.meta_task import *

with DAG(
    dag_id='etl_meta_info',
    start_date=datetime(2025, 11, 19),
    schedule='0 0 * * *',
    catchup=False,
    tags=['openalex'],
    default_args={
        'retries': 20,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    s3_bucket = 'logicat-dailypaper'
    s3_key    = 'openalex_field.csv'
    schema    = 'raw_data'

    with TaskGroup(group_id="meta_3days_field_fetch") as meta_3days_field:
        field_df = read_field_csv(s3_bucket=s3_bucket,s3_key=s3_key,aws_conn_id='logicat_aws_conn')
        meta_3days_field = meta_field_3_days_fetch(field_df=field_df)
        load_3days_field(meta_3days_field, schema, table='meta_3days_field')
        
    with TaskGroup(group_id="meta_daily_fetch") as meta_daily:
        meta_daily = meta_daily_fetch()
        load_daily(meta_daily, schema, table='meta_daily')
    
    with TaskGroup(group_id="meta_main_fetch") as meta_main:
        meta_main = meta_main_fetch()
        load_main(meta_main, schema, table='meta_main')
        
    with TaskGroup(group_id="meta_type_fetch") as meta_type:
        meta_type = meta_type_fetch()
        load_type(meta_type, schema, table='meta_type')
        
    with TaskGroup(group_id="meta_field_fetch") as meta_field:
        field_df = read_field_csv(s3_bucket=s3_bucket,s3_key=s3_key,aws_conn_id='logicat_aws_conn')
        meta_field = meta_field_fetch(field_df=field_df)
        load_field(meta_field, schema, table='meta_field')