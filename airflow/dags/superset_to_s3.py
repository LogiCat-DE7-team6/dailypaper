from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task

from scripts.superset_crawler import crawl_superset_dashboard

from datetime import datetime, timedelta
import logging


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
    
    main_key = f"main/{execution_date_str}_main.png"
    field_key = f"field/{execution_date_str}_field.png"

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

with DAG(
    dag_id='superset_to_s3',
    start_date=datetime(2025, 11, 17), 
    schedule='5 0 * * *', 
    is_paused_upon_creation=True,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    main_path = '/tmp/main.png'
    field_path = '/tmp/field.png'

    s3_bucket='logicat-dailypaper'
    execution_date_str = "{{ ds }}"

    output_paths = crawling_task(main_path, field_path)

    images_to_s3(output_paths, s3_bucket, execution_date_str, aws_conn_id='logicat_aws_conn')
    