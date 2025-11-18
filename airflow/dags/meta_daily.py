from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from scripts.meta_fetch import daily_fetch

from datetime import datetime, timedelta
import logging

def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='logicat_snowflake_conn')
    return hook.get_conn().cursor()

@task
def meta_daily_fetch(**kwargs):

    api_url = Variable.get("openalex_url")
    mailto = Variable.get("openalex_mailto")
    execution_date = kwargs['execution_date']

    meta_main = daily_fetch(execution_date, mailto, api_url)
    return meta_main

@task
def load(rows, schema, table):
    cur = get_snowflake_connection()
    
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        execution_date DATE,
        publication_date DATE,
        total BIGINT,
        physical_sciences BIGINT,
        life_sciences BIGINT,
        health_sciences BIGINT,
        social_sciences BIGINT,
        unknown BIGINT
    );"""

    logging.info(create_table_sql)

    try:
        cur.execute(create_table_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    
    for row in rows:
        # 중복 데이터 체크
        check_sql = f"""
        SELECT COUNT(*) FROM {schema}.{table}
        WHERE execution_date = '{row['execution_date']}' 
          AND publication_date = '{row['publication_date']}';
        """
        logging.info(check_sql)
        cur.execute(check_sql)
        count = cur.fetchone()[0]

        if count == 0:
            insert_sql = f"""
            INSERT INTO {schema}.{table} 
            (execution_date, publication_date, total, physical_sciences, life_sciences, 
            health_sciences, social_sciences, unknown)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                row.get('execution_date'),
                row.get('publication_date'),
                row.get('total', 0),
                row.get('Physical_Sciences', 0),
                row.get('Life_Sciences', 0),
                row.get('Health_Sciences', 0),
                row.get('Social_Sciences', 0),
                row.get('unknown', 0)
            )

            try:
                cur.execute(insert_sql, values)
                cur.execute("COMMIT;")
            except Exception as e:
                cur.execute("ROLLBACK;")
                raise

with DAG(
    dag_id='meta_daily_fetch',
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
    table = 'meta_daily'

    meta_daily = meta_daily_fetch()
    load(meta_daily, schema, table)