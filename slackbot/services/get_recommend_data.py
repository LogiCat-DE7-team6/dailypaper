from utils.snowflake_conn import sf_pool
from psycopg2 import DatabaseError

def get_recommend_data():
    conn = sf_pool.get_connection()
    sql = [
        """
        SELECT MAX(execution_date) FROM slack.recommendations; 
        """,
        """
        SELECT 
            url
        FROM slack.recommendations
        WHERE type = '인용논문'
            AND execution_date = (SELECT MAX(execution_date) FROM slack.recommendations);
        """,
        """
        SELECT 
            keyword
        FROM slack.recommendations
        WHERE type = '키워드'
            AND execution_date = (SELECT MAX(execution_date) FROM slack.recommendations);
        """,
        """
        SELECT 
            url
        FROM slack.recommendations
        WHERE type = '키워드'
            AND execution_date = (SELECT MAX(execution_date) FROM slack.recommendations);
        """,
        """
        SELECT 
            url
        FROM slack.recommendations
        WHERE type = '하이라이트'
            AND execution_date = (SELECT MAX(execution_date) FROM slack.recommendations);
        """
    ]
    try:
        data = []
        cur = conn.cursor()
        for i in range(len(sql)):
            cur.execute(sql[i])
            result = cur.fetchall()
            if i == 1 or i == 4:
                result_str = ",".join(item[0] for item in result)
                data.append(result_str)
            else:
                data.append(result[0][0])
        return data
    except DatabaseError as e:
        raise e
    finally:
        cur.close()
        sf_pool.release_connection(conn)