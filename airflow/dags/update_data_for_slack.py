from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task

from scripts.superset_crawler import crawl_superset_dashboard

from datetime import datetime, timedelta
import logging

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor


# airflow dags test merge_data_for_slack 2025-11-17

with DAG(
    dag_id = 'update_data_for_slack',
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

    @task
    def crawling_task(main_path, field_path):
        url = Variable.get("superset_dailypaper_url")
        id = Variable.get("superset_id")
        pw = Variable.get("superset_pw")
        tabs = {
            "TAB-t_udG7qJ8ZxcGjDWoliOH": main_path,
            "TAB-c-OjEJBM44jEk2N7ioK-X": field_path
        }
        saved_paths = crawl_superset_dashboard(url, id, pw, tabs)
        return {"main": saved_paths["TAB-t_udG7qJ8ZxcGjDWoliOH"], "field": saved_paths["TAB-c-OjEJBM44jEk2N7ioK-X"]}

    @task
    def images_to_s3(path_dict, s3_bucket, execution_date_str, aws_conn_id):
        logging.info("S3 이미지 업로드 작업을 시작합니다.")
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        main_path = path_dict["main"]
        field_path = path_dict["field"]
        
        # main, field -> test
        main_key = f"test/{execution_date_str}_main.png"
        field_key = f"test/{execution_date_str}_field.png"

        s3_hook.load_file(
            filename=main_path,
            key=main_key,
            bucket_name=s3_bucket,
            replace=True
        )
        logging.info(f"{main_key} S3 업로드 완료")

        s3_hook.load_file(
            filename=field_path,
            key=field_key,
            bucket_name=s3_bucket,
            replace=True
        )
        logging.info(f"{field_key} S3 업로드 완료")

        return True
    
    main_path = '/tmp/main.png'
    field_path = '/tmp/field.png'

    s3_bucket='logicat-dailypaper'
    execution_date_str = "{{ ds }}"

    output_paths = crawling_task(main_path, field_path)
    s3_upload_task = images_to_s3(output_paths, s3_bucket, execution_date_str, aws_conn_id='logicat_aws_conn')

    sensors = [wait_for_meta, wait_for_recom]

    for t in [task1, task2, task3]:
        sensors >> t
    
    sensors >> output_paths >> s3_upload_task