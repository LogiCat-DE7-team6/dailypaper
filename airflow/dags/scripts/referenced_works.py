from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import get_current_context

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
from pyalex import Works
from collections import Counter
import json

TOP_N = 3
FETCH_TOP_N = 10

log = LoggingMixin().log

# ===================================
# 3. S3에서 7일치 데이터 읽고 top N 논문 선택
# ===================================
@task
def pick_top_from_s3(date_list, bucket, base_path, aws_conn_id):
    context = get_current_context()
    execution_date = context["ds"]
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    ref_list = []
    counter = Counter()

    # 1) S3에서 일자별 파일 읽기
    for d in date_list:
        key = f"{base_path}/{execution_date}/work_{execution_date}_{d}.json"
        log.info(f"[pick_top_from_s3] Reading S3 key={key}")

        try:
            content = s3.read_key(key=key, bucket_name=bucket)
        except Exception as e:
            raise AirflowFailException(f"Failed to read S3 key={key}: {e}")

        if not content:
            raise AirflowFailException(f"Missing S3 content for key={key}")

        try:
            items = json.loads(content)
        except Exception as e:
            raise AirflowFailException(f"Invalid JSON file: {key}: {e}")

        # 2) 각 work에 있는 referenced_works를 모아 리스트에 추가
        for w in items:
            refs = w.get("referenced_works") or []
            #ref_list.extend(refs)
            counter.update(refs)

    #log.info(f"[pick_top_from_s3] Total referenced IDs collected={len(ref_list)}")
    # 3) referenced id 빈도수 계산
    #counter = Counter(ref_list)

    # 4) 상위 N개 referenced id 추출
    top_refs = [
        {"work_id": ref_id, "ref_cnt_in_source": count}
        for ref_id, count in counter.most_common(FETCH_TOP_N)
    ]
    log.info(f"[pick_top_from_s3] Top {FETCH_TOP_N} references = {counter.most_common(FETCH_TOP_N)}")

    return top_refs


# ===================================
# 4. top N 논문 상세 재조회
# ===================================
@task
def fetch_details(top_items):
    def safe_int(value):
        try:
            return int(value) if value is not None else None
        except:
            return None
    
    def safe_float(value):
        try:
            return float(value) if value is not None else None
        except:
            return None

    def safe_bool(value):
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        s = str(value).lower()
        if s in ["true", "1", "yes"]:
            return True
        if s in ["false", "0", "no"]:
            return False
        return None
        
    def safe_str(value):
        if value is None:
            return None
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    context = get_current_context()
    execution_date = context["ds"]

    results = []

    for item in top_items:
        work_id = item["work_id"]
        ref_cnt_in_source = item["ref_cnt_in_source"]

        try:
            w = Works()[work_id]
        except:
            continue

        primary_topic = w.get("primary_topic") or {}

        domain_obj = primary_topic.get("domain") or {}
        field_obj = primary_topic.get("field") or {}
        subfield_obj = primary_topic.get("subfield") or {}

        record = {
            "id": work_id,
            "title": safe_str(w.get("display_name")),
            "doi": safe_str(w.get("doi")),
            "publication_date": safe_str(w.get("publication_date")),
            "type": safe_str(w.get("type")),
            "is_oa": safe_bool(w.get("is_oa")),
            
            "domain": domain_obj.get("display_name"),
            "field": field_obj.get("display_name"),
            "subfield": subfield_obj.get("display_name"),

            "citations_past_decade": safe_int(
                sum(
                    safe_int(row.get("cited_by_count", 0))
                    for row in (w.get("counts_by_year") or [])
                    if safe_int(row.get("year", 0)) is not None
                )
            ),
            "keywords": ", ".join(
                k.get("display_name") for k in (w.get("keywords") or []) if k.get("display_name")
            ),
            "fwci": safe_float(w.get("fwci")),
            "ref_cnt_in_source": ref_cnt_in_source,
            "execution_date": execution_date,
        }

        results.append(record)
        if len(results) == TOP_N:
            break

    return results


# ===================================
# 5. Snowflake 적재
# ===================================
@task
def load_to_snowflake(records, schema, table, snowflake_conn_id):
    if not records:
        print("No data to insert.")
        return

    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()

    log.info(f"[load_to_snowflake] Start loading {len(records)} records into Snowflake.")

    try:
        # =========================================
        # 1) 테이블 생성 (없다면)
        # =========================================
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                ID VARCHAR,
                TITLE VARCHAR,
                DOI VARCHAR,
                PUBLICATION_DATE DATE,
                TYPE VARCHAR,
                IS_OA BOOLEAN,
                DOMAIN VARCHAR,
                FIELD VARCHAR,
                SUBFIELD VARCHAR,
                CITATIONS_PAST_DECADE INTEGER,
                KEYWORDS VARCHAR,
                FWCI FLOAT,
                REF_CNT_IN_SOURCE INTEGER,
                EXECUTION_DATE DATE
            );
        """
        cur.execute(create_sql)
        log.info("[load_to_snowflake] Ensured table exists.")

        # =========================================
        # 2) INSERT 문 준비
        # =========================================
        insert_sql = f"""
            INSERT INTO {schema}.{table} (
                ID,
                TITLE,
                DOI,
                PUBLICATION_DATE,
                TYPE,
                IS_OA,
                DOMAIN,
                FIELD,
                SUBFIELD,
                CITATIONS_PAST_DECADE,
                KEYWORDS,
                FWCI,
                REF_CNT_IN_SOURCE,
                EXECUTION_DATE
            )
            VALUES (
                %(id)s,
                %(title)s,
                %(doi)s,
                %(publication_date)s,
                %(type)s,
                %(is_oa)s,
                %(domain)s,
                %(field)s,
                %(subfield)s,
                %(citations_past_decade)s,
                %(keywords)s,
                %(fwci)s,
                %(ref_cnt_in_source)s,
                %(execution_date)s
            );
        """

        # =========================================
        # 3) 트랜잭션 시작
        # =========================================
        conn.autocommit = False

        # =========================================
        # 4) 레코드 반복 INSERT
        # =========================================
        for r in records:
            log.debug(f"[load_to_snowflake] Inserting ID={r['id']}")
            cur.execute(insert_sql, r)

        # =========================================
        # 5) 커밋
        # =========================================
        conn.commit()
        log.info(f"[load_to_snowflake] Inserted {len(records)} rows successfully.")
        #print(f"{len(records)} records successfully inserted into Snowflake.")

    except Exception as e:
        # 실패 시 롤백
        conn.rollback()
        print("❌ Snowflake INSERT failed. All changes rolled back.")
        raise e

    finally:
        # 커서 및 연결 해제
        cur.close()
        conn.close()