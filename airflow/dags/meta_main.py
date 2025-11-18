from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from scripts.meta_fetch import main_fetch

from datetime import datetime, timedelta
import logging

def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='logicat_snowflake_conn')
    return hook.get_conn().cursor()

@task
def meta_main_fetch(**kwargs):

    api_url = Variable.get("openalex_url")
    mailto = Variable.get("openalex_mailto")
    execution_date = kwargs['execution_date']

    meta_main = main_fetch(execution_date, mailto, api_url)
    return meta_main

@task
def load(meta_main, schema, table):
    cur = get_snowflake_connection()
    
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
    execution_date DATE,
    total BIGINT,
    physical_sciences BIGINT,
    life_sciences BIGINT,
    health_sciences BIGINT,
    social_sciences BIGINT,
    unknown BIGINT,
    oa_true BIGINT
    );"""

    logging.info(create_table_sql)

    try:
        cur.execute(create_table_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    

    # 중복 데이터 체크
    check_sql = f"""
    SELECT COUNT(*) FROM {schema}.{table}
    WHERE execution_date = '{meta_main['execution_date']}';
    """
    logging.info(check_sql)

    cur.execute(check_sql)
    count = cur.fetchone()[0]
    
    # 중복이 아니라면 데이터 삽입
    if count == 0:
        insert_sql = f"""
        INSERT INTO {schema}.{table} 
        (execution_date, total, physical_sciences, life_sciences, 
        health_sciences, social_sciences, unknown, oa_true)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
            meta_main.get('execution_date'),
            meta_main.get('total', 0),
            meta_main.get('physical_sciences', 0),
            meta_main.get('life_sciences', 0),
            meta_main.get('health_sciences', 0),
            meta_main.get('social_sciences', 0),
            meta_main.get('unknown', 0),
            meta_main.get('oa_true', 0)
        )
        try:
            cur.execute(insert_sql, values)
            cur.execute("COMMIT;")

        except Exception as e:
            cur.execute("ROLLBACK;")
            raise

with DAG(
    dag_id='meta_main_fetch',
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

    schema = 'raw_data'
    table = 'meta_main'
    meta_main = meta_main_fetch()
    load(meta_main, schema, table)