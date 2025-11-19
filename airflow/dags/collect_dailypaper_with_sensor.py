from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor


# airflow dags test merge_data_for_slack 2025-11-17

with DAG(
    dag_id = 'merge_data_for_slack',
    start_date=datetime(2025, 11, 19),
    schedule = '0 0 * * *', # 매일 0시 0분에 실행
    catchup = False,
    tags=['openalex', 'sensor'],
    default_args={
        'retries': 20,
        'retry_delay': timedelta(minutes=3),
    }
) as dag :
    
    wait_for_meta = ExternalTaskSensor(
        task_id="wait_for_meta",
        external_dag_id="etl_meta_info",   # 기다릴 DAG 이름
        external_task_id=None,     # None이면 DAG 전체 성공을 기다림
        poke_interval=60,          # 60초마다 확인
        timeout=3600*2,            # 최대 2시간 기다림
        mode="reschedule",
        execution_date_fn=lambda x: x
    )

    wait_for_recom = ExternalTaskSensor(
        task_id="wait_foretl_recommend_works_recom",
        external_dag_id="etl_recommend_works",   # 기다릴 DAG 이름
        external_task_id=None,     # None이면 DAG 전체 성공을 기다림
        poke_interval=60,          # 60초마다 확인
        timeout=3600*2,            # 최대 2시간 기다림
        mode="reschedule",
        execution_date_fn=lambda x: x
    )
    
    # 모든 DAG가 완료된 이후 실행
    task1 = SnowflakeOperator(
        task_id="run_recommend_union_query",
        snowflake_conn_id="logicat_snowflake_conn",
        sql="sql/recommend_union_query.sql",
        params={
            "ds": "{{ ds }}"
        }
    )

    task2 = SnowflakeOperator(
        task_id="slack_main_query",
        snowflake_conn_id="logicat_snowflake_conn",
        sql="sql/slack_main_query.sql",
    )
    
    task3 = SnowflakeOperator(
        task_id="slack_type_query",
        snowflake_conn_id="logicat_snowflake_conn",
        sql="sql/slack_type_query.sql",
    )
    
    for t in [task1, task2, task3]:
        [wait_for_meta, wait_for_recom] >> t