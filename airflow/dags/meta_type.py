from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from scripts.meta_fetch import type_fetch

from datetime import datetime, timedelta
import logging

def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='logicat_snowflake_conn')
    return hook.get_conn().cursor()

@task
def meta_type_fetch(**kwargs):

    api_url = Variable.get("openalex_url")
    mailto = Variable.get("openalex_mailto")
    execution_date = kwargs['execution_date']

    meta_type = type_fetch(execution_date, mailto, api_url)
    return meta_type

@task
def load(meta_type, schema, table):
    cur = get_snowflake_connection()
    
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        execution_date DATE,
        article BIGINT,
        book_chapter BIGINT,
        dataset BIGINT,
        preprint BIGINT,
        dissertation BIGINT,
        book BIGINT,
        review BIGINT,
        paratext BIGINT,
        other BIGINT,
        libguides BIGINT,
        letter BIGINT,
        reference_entry BIGINT,
        report BIGINT,
        peer_review BIGINT,
        editorial BIGINT,
        erratum BIGINT,
        standard BIGINT,
        "grant" BIGINT,
        supplementary_materials BIGINT,
        retraction BIGINT,
        book_section BIGINT,
        "database" BIGINT,
        software BIGINT,
        report_component BIGINT
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
    WHERE execution_date = '{meta_type['execution_date']}';
    """
    logging.info(check_sql)

    cur.execute(check_sql)
    count = cur.fetchone()[0]
    
    # 중복이 아니라면 데이터 삽입
    if count == 0:
        insert_sql = f"""
        INSERT INTO {schema}.{table} 
        (execution_date, article, book_chapter, dataset, preprint, dissertation, book, review,
         paratext, other, libguides, letter, reference_entry, report, peer_review, editorial,
         erratum, standard, "grant", supplementary_materials, retraction, book_section,
         "database", software, report_component)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        values = (
            meta_type.get('execution_date'),
            meta_type.get('article', 0),
            meta_type.get('book_chapter', 0),
            meta_type.get('dataset', 0),
            meta_type.get('preprint', 0),
            meta_type.get('dissertation', 0),
            meta_type.get('book', 0),
            meta_type.get('review', 0),
            meta_type.get('paratext', 0),
            meta_type.get('other', 0),
            meta_type.get('libguides', 0),
            meta_type.get('letter', 0),
            meta_type.get('reference_entry', 0),
            meta_type.get('report', 0),
            meta_type.get('peer_review', 0),
            meta_type.get('editorial', 0),
            meta_type.get('erratum', 0),
            meta_type.get('standard', 0),
            meta_type.get('grant', 0),
            meta_type.get('supplementary_materials', 0),
            meta_type.get('retraction', 0),
            meta_type.get('book_section', 0),
            meta_type.get('database', 0),
            meta_type.get('software', 0),
            meta_type.get('report_component', 0)
            )
        try:
            cur.execute(insert_sql, values)
            cur.execute("COMMIT;")

        except Exception as e:
            cur.execute("ROLLBACK;")
            raise

with DAG(
    dag_id='meta_type_fetch',
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
    table = 'meta_type'

    meta_type = meta_type_fetch()
    load(meta_type, schema, table)