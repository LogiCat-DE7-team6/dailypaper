from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
    
with DAG(
    dag_id = 'test_snowflake_connection',
    start_date = datetime(2025, 11, 15),
    schedule = None, # 매일 0시 0분에 실행
    catchup = False,

) as dag :
    # @task
    # def test_conn_task():
    #     hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    #     conn = hook.get_conn()
    #     cursor = conn.cursor()
    #     cursor.execute("SELECT CURRENT_VERSION();")
    #     cursor.execute("SELECT * FROM processed.temp_table;")
    #     print("Snowflake Version:", cursor.fetchone())
    
    # test_conn_task()

    test_conn = SnowflakeOperator(
        task_id="test_conn",
        snowflake_conn_id="logicat_snowflake_conn",
        sql="""INSERT INTO processed.temp_table (tmp_data_id, amount) VALUES
                ('id_001', 100),
                ('id_002', 200),
                ('id_003', 300);"""
    )