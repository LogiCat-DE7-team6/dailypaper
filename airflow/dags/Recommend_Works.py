from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from datetime import datetime, timedelta

from scripts.extract_works import make_date_list, extract_work, upload_file_to_s3, delete_tempfile
from scripts.highlight import get_highlight_works
from scripts.title_keyword import get_title_based_works
from scripts.snowflake import insert_to_snowflake
from scripts.referenced_works import pick_top_from_s3, fetch_details, load_to_snowflake

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

with DAG(
    dag_id = 'etl_recommend_works',
    start_date=datetime(2025, 11, 19),
    schedule = '0 0 * * *', # 매일 0시 0분에 실행
    catchup = False,
    tags=['openalex'],
    default_args={
        'retries': 20,
        'retry_delay': timedelta(minutes=3),
    }
) as dag :
    ## 1. Work 수집
    with TaskGroup(group_id="extract_recently_published_works") as extract_group:
        # 수집할 date 기간 생성, ['2025-11-01', '2025-11-02' ...]
        dates = make_date_list(lookback_days=7)
        
        # Dynamic Task : 각 date별 work 데이터 수집 및 임시 json 파일 컨테이너 내부에 저장
        tmp_fnames = extract_work\
                        .partial(base_path=f"{Variable.get('DATA_DIR')}/tmp/work")\
                        .expand(p_date=dates)
        
        # Dynamic Task : 임시 json 파일을 S3에 업로드
        uploaded = upload_file_to_s3\
            .partial(base_path='recommend/work'
                    , bucket='logicat-dailypaper'
                    , aws_conn_id="logicat_aws_conn")\
            .expand(input_fname=tmp_fnames)
            
        # Dynamic Task : 임시 json 파일 삭제
        uploaded >> delete_tempfile.expand(input_fname = tmp_fnames)
    
    ## 2. Highlight 논문용 데이터 변환및 Snowflake 적재
    with TaskGroup(group_id="transform_for_highlight") as transform_group_1:
        res_highlight = get_highlight_works(base_path='recommend/work'
                            , bucket='logicat-dailypaper'
                            , aws_conn_id="logicat_aws_conn")
        
        insert_to_snowflake(res_highlight
                            , schema="processed"
                            , table="highlight"
                            , conn_id="logicat_snowflake_conn")
        
    ## 3. Keyword 분석용 데이터 변환 및 Snowflake 적재
    with TaskGroup(group_id="transform_for_keyword") as transform_group_2:
        res_keywords = get_title_based_works(
                            base_path='recommend/work'
                            , bucket='logicat-dailypaper'
                            , aws_conn_id="logicat_aws_conn"
                        )
        
        insert_to_snowflake(res_keywords
                            , schema="processed"
                            , table="title_keyword"
                            , conn_id="logicat_snowflake_conn"
                            )
        
    ## 3. 인용논문 데이터 변환 및 Snowflake 적재
    with TaskGroup(group_id="transform_for_ref") as transform_group_3:
        dates = make_date_list(lookback_days=7)
        
        top_ids = pick_top_from_s3(
                    date_list=dates,
                    bucket='logicat-dailypaper',
                    base_path='recommend/work',
                    aws_conn_id="logicat_aws_conn")
        
        details = fetch_details(top_items=top_ids)
        
        load_to_snowflake(records=details,
            schema="processed",
            table="ref_work",
            snowflake_conn_id="logicat_snowflake_conn")

    extract_group >> [transform_group_1, transform_group_2, transform_group_3]