import json, os, pyalex, time
from datetime import datetime, timedelta
from pyalex import Works

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

def fetch_with_retry(query, cursor, max_try=5):
    # max_try 만큼 get 시도 -> 실패할 경우 raise를 통해 dag의 retry 되도록
    for attempt in range(1, max_try + 1):
        try:
            return query.get(per_page=200, cursor=cursor)
        except Exception as e:
            if attempt == max_try:
                print(f"Attempt {attempt} failed: {e}")
                raise
            time.sleep(5)

@task
def make_date_list(lookback_days,**context):
    return [(context['execution_date'] - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(0, lookback_days)][::-1]

@task
def extract_work(p_date, base_path, **context):
    logger.info(f"Current extracting Date={p_date}")
    pyalex.config.email = Variable.get("pyalex_email")
    
    query = Works().filter(publication_date=p_date)\
        .select([
            "id", "display_name", "doi", "publication_date",
            "primary_topic", "referenced_works", "fwci"
        ])
    
    if query.count() == 0 :
        # work 수가 0개이면 Skip 처리
        raise AirflowSkipException(f"No data - {p_date}")
        
    results = []
    next_cursor = "*"
    while next_cursor is not None:
        r = fetch_with_retry(query, next_cursor)
        results.extend(r)
        next_cursor = r.meta["next_cursor"]
        time.sleep(1)

    logger.info(f"Num of works = {len(results)}")
    # 컨테이너 내부에 임시 json 파일 적재
    os.makedirs(f"{base_path}/{context['ds']}", exist_ok=True)  # 디렉토리 없으면 생성
    fname = f"{base_path}/{context['ds']}/work_{context['ds']}_{p_date}.json"
    logger.info(f"Saving Work data - {fname}...")
    with open(fname, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False)
    
    logger.info(f"Complete Save Work data - {fname}")
    return fname

@task 
def upload_file_to_s3(input_fname, base_path, bucket, aws_conn_id='aws_s3_conn_id' ,**context):
    '''
    컨테이너 내부에 저장된 JSON to S3
    '''
    if not os.path.exists(input_fname):
        raise AirflowSkipException(f"No Json File - {input_fname}")
    fname = os.path.basename(input_fname)
    
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_file(
        filename=input_fname,
        key=f"{base_path}/{context['ds']}/{fname}",
        bucket_name=bucket,
        replace=True,
    )
    logger.info(f"[{context['ds']}] Successfully Data uploaded. s3_name = {base_path}/{context['ds']}/{fname}")

@task
def delete_tempfile(input_fname, **context):
    if os.path.exists(input_fname):
        os.remove(input_fname)
        logger.info(f"Successfully File Deleted - {input_fname}")
