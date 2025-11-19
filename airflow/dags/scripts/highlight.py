import pandas as pd
from utils import s3_utils

from airflow.decorators import task

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

def get_df_4_highlight(data):
    # 논문 하이라이트에 필요한 요소 추출 및 null 값 처리
    ids    = [w.get("id") for w in data]
    dois   = [w.get("doi") for w in data]
    titles = [w.get("display_name") for w in data]
    publication_dates = [w.get("publication_date") for w in data]
    fwcis = [w.get("fwci") for w in data]
    domain_ids   = [w.get("primary_topic").get('domain').get('id').split("/")[-1]
                    if w.get("primary_topic") != None else None
                    for w in data ]
    domain_names = [w.get("primary_topic").get('domain').get('display_name')
                    if w.get("primary_topic") != None else None
                    for w in data ]
    
    df = pd.DataFrame({
        'openalex_url': ids,
        'doi': dois,
        'title': titles,
        'publication_date': publication_dates,
        'fwci': fwcis,
        'domain_id': domain_ids,
        'domain_name' : domain_names
    })
    df['work_id']     = df['openalex_url'].apply(lambda x: x.split('/')[-1])
    df['domain_id']   = df['domain_id'].fillna('None')
    df['domain_name'] = df['domain_name'].fillna('')
    return df 

@task
def get_highlight_works(base_path, bucket, aws_conn_id="aws_s3_conn_id", **context):
    # 적재된 json 파일들 load 및 dataframe으로 변환
    prefix = f"{base_path}/{context['ds']}/"
    all_data = s3_utils.load_all_jsons(bucket, prefix, aws_conn_id=aws_conn_id)
    df = get_df_4_highlight(all_data)
    
    logging.info(f"[시작] FWCI 기준 데이터 정렬 및 1위 선정")
    # fwci 값이 가장 큰 work 추출
    # 동일 점수가 있으면 가장 과거에 출간된 것을 기준으로함.
    df["row_number"] = (
        df.sort_values(["domain_id", "domain_name", "fwci", "publication_date"], ascending=[True, False, False, True])
        .groupby(["domain_id", "domain_name"])
        .cumcount() + 1
    )
    h_df = df[df['row_number']==1].drop(columns=['row_number']).reset_index(drop=True)
    h_df['execution_date'] = [context['ds'],] * len(h_df)
    logging.info(f"[종료] FWCI 기준 데이터 정렬 및 1위 선정")
    print(h_df)

    return h_df.to_dict('records')