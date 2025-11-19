import os
from dotenv import load_dotenv
from psycopg2 import pool, DatabaseError

class DBConn():
    def __init__(self, host, database, user, password, port=5432, min_conn=1, max_conn=5):
        try:
            self.pool = pool.SimpleConnectionPool(
                min_conn, 
                max_conn,
                host = host,
                database = database,
                user = user,
                password = password,
                port = port,
            )
            print("Local DB pool 생성 완료")
        except DatabaseError as e:
            raise e

    def get_connection(self):
        return self.pool.getconn()

    def release_connection(self, conn):
        self.pool.putconn(conn)
    
    def create_user_data(self):
        self.pool.closeall()


load_dotenv()
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_database = os.getenv("DB_DATABASE")



db_pool = DBConn(
            host=db_host, # docker 이미지를 빌드해서 사용시 같은 network에 있어야함. 이떄 host는 db 컨테이너의 이름 혹은 컨테이너의 ip주소를 입력한다.
            database=db_database,
            user=db_user,
            password=db_password
            )
