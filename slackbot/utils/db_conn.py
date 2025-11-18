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
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


db_pool = DBConn(
            host='localhost',
            database='local-practice',
            user=db_user,
            password=db_password
            )
