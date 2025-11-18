from utils.db_conn import db_pool
from utils.snowflake_conn import sf_pool
from psycopg2 import DatabaseError

def select_daily_data():
    conn = sf_pool.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
           "SELECT * FROM raw_data.meta_main WHERE execution_date = '2025-11-17' ;"
        )

        data = cur.fetchall()
        return data
    except DatabaseError as e:
        raise e
    finally:
        cur.close()
        sf_pool.release_connection(conn)