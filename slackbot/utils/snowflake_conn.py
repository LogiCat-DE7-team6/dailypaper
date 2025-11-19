import os
from dotenv import load_dotenv
import snowflake.connector
from psycopg2 import DatabaseError

class SnowFlakeConn():
    def __init__(self, user, password, account, warehouse, database, schema=None, min_conn=1, max_conn=5):
        try:
            self.pool = []
            
            for _ in range(min_conn, max_conn):
                conn = snowflake.connector.connect(
                    user=user,
                    password=password,
                    account=account,
                    warehouse=warehouse,
                    database=database
                )
                self.pool.append(conn)

            print("SnowFlake pool 생성 완료")
        except DatabaseError as e:
            raise e

    def get_connection(self):
        if self.pool:
            return self.pool.pop(0)

    def release_connection(self, conn):
        self.pool.append(conn)
    
    def create_user_data(self):
        for conn in self.pool:
            conn.close()
        print("Snowflake connection pool closed.")


load_dotenv()
sf_user = os.getenv("SNOW_USER")
sf_password = os.getenv("SNOW_PWD")
sf_warehouse = os.getenv("WAREHOUSE")
sf_account = os.getenv("SNOW_ACCOUNT")
sf_database = os.getenv("SNOW_DB")

sf_pool = SnowFlakeConn(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_warehouse,
            database=sf_database # test용 - dev, 서비스용 - dailypaper
        )

#cur.commit() -> snowflake는 기본적으로 autocommit을 지원하기 떄문에 commit()하지 않아도 됌. 사용하려면 pool 만들때 autocommit=False 옵션 주면 된다
