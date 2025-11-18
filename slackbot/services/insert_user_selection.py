from utils.db_conn import db_pool
from psycopg2 import DatabaseError


def insert_user_selection(user, selected_option):
    conn = db_pool.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
           "INSERT INTO dev_test.user_select_table (user_id, answer) VALUES (%s, %s)",
            (user, selected_option)
        )
        conn.commit()
        print("user data 저장 완료")
    except DatabaseError as e:
        raise e
    finally:
        cur.close()
        db_pool.release_connection(conn)

