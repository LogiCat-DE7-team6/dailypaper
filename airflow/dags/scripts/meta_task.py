from airflow.decorators import task
from airflow.models import Variable

import  logging

from scripts.meta_fetch import *
from utils.meta_utils import *


# ====== functions for meta_3days_field_fetch task

@task
def meta_field_3_days_fetch(field_df, **kwargs):

    api_url = Variable.get("openalex_url")
    mailto = Variable.get("openalex_mailto")
    execution_date = kwargs['execution_date']

    meta_field = field_3_days_fetch(execution_date, mailto, api_url, field_df)
    return meta_field

@task
def load_3days_field(rows, schema, table):
    cur = get_snowflake_connection()
    
    # 테이블 생성
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        execution_date DATE,
        start_date DATE,
        end_date DATE,
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
            INSERT INTO {schema}.{table} (execution_date, start_date, end_date, domain, field, count)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            values = (
                row['execution_date'],
                row['start_date'],
                row['end_date'],
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
            
# ====== functions for meta_daily_fetch task
@task
def meta_daily_fetch(**kwargs):

    api_url = Variable.get("openalex_url")
    mailto = Variable.get("openalex_mailto")
    execution_date = kwargs['execution_date']

    meta_main = daily_fetch(execution_date, mailto, api_url)
    return meta_main

@task
def load_daily(rows, schema, table):
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
            
# ====== functions for meta_main_fetch task
@task
def meta_main_fetch(**kwargs):

    api_url = Variable.get("openalex_url")
    mailto = Variable.get("openalex_mailto")
    execution_date = kwargs['execution_date']

    meta_main = main_fetch(execution_date, mailto, api_url)
    return meta_main

@task
def load_main(meta_main, schema, table):
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

# ====== functions for meta_type_fetch task
@task
def meta_type_fetch(**kwargs):
    api_url = Variable.get("openalex_url")
    mailto = Variable.get("openalex_mailto")
    execution_date = kwargs['execution_date']

    meta_type = type_fetch(execution_date, mailto, api_url)
    return meta_type

@task
def load_type(meta_type, schema, table):
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

# ==== functions for meta_field_fetch task
@task
def meta_field_fetch(field_df, **kwargs):

    api_url = Variable.get("openalex_url")
    mailto = Variable.get("openalex_mailto")
    execution_date = kwargs['execution_date']

    meta_field = field_fetch(execution_date, mailto, api_url, field_df)
    return meta_field

@task
def load_field(rows, schema, table):
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