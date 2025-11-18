from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task

from scripts.meta_fetch import field_fetch

from datetime import datetime, timedelta
import logging
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

@task
def meta_field_fetch(field_df, **kwargs):

    api_url = Variable.get("openalex_url")
    mailto = Variable.get("openalex_mailto")
    execution_date = kwargs['execution_date']

    meta_field = field_fetch(execution_date, mailto, api_url, field_df)
    return meta_field

@task
def load(rows, schema, table):
    cur = get_snowflake_connection()
    
    # 테이블 생성
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        execution_date DATE,
        domain VARCHAR(100),
        field VARCHAR(100),
        count BIGINT
    );"""
    logging.info(create_table_sql)
    
    try:
        cur.execute(create_table_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    
    for row in rows:
        # 중복 체크: execution_date + field 기준
        check_sql = f"""
        SELECT COUNT(*) FROM {schema}.{table}
        WHERE execution_date = '{row['execution_date']}' 
          AND field = '{row['field']}';
        """
        logging.info(check_sql)
        cur.execute(check_sql)
        count = cur.fetchone()[0]
        
        if count == 0:
            insert_sql = f"""
            INSERT INTO {schema}.{table} (execution_date, domain, field, count)
            VALUES (%s, %s, %s, %s);
            """
            values = (
                row['execution_date'],
                row['domain'],
                row['field'],
                row['count']
            )
            try:
                cur.execute(insert_sql, values)
                cur.execute("COMMIT;")
            except Exception as e:
                cur.execute("ROLLBACK;")
                raise

with DAG(
    dag_id='meta_field_fetch',
    start_date=datetime(2025, 11, 16),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 0 * * *',  # 적당히 조절
    is_paused_upon_creation=True,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    s3_bucket='logicat-dailypaper'
    s3_key='openalex_field.csv'
    schema = 'raw_data'
    table = 'meta_field'

    field_df = read_field_csv(s3_bucket=s3_bucket,s3_key=s3_key,aws_conn_id='logicat_aws_conn')
    meta_field = meta_field_fetch(field_df=field_df)
    load(meta_field, schema, table)