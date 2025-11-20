from utils.snowflake_conn import sf_pool
from psycopg2 import DatabaseError
import utils.get_sql as sqls

def get_data(button_value):
    conn = sf_pool.get_connection()
    sql = getattr(sqls, button_value)

    if button_value == "recommend":
        try:
            data = []
            cur = conn.cursor()
            for i in range(len(sql)):
                cur.execute(sql[i])
                result = cur.fetchall()
                data.append(result)
            return data
        except DatabaseError as e:
            raise e
        finally:
            cur.close()
            sf_pool.release_connection(conn)

    else:
        try:
            data = []
            cur = conn.cursor()
            for i in range(len(sql)):
                cur.execute(sql[i])
                result = cur.fetchall()
                data.append(result[0][0])
            return data
        
        except DatabaseError as e:
            raise e
        finally:
            cur.close()
            sf_pool.release_connection(conn)
            