import re, random
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from utils import s3_utils
from lingua import Language, LanguageDetectorBuilder
from airflow.decorators import task
from pyalex import Works

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

def get_df_4_title_keyword(data):
    titles      = [w.get("display_name") for w in data]
    domain_ids   = [w.get("primary_topic").get('domain').get('id').split("/")[-1]
                    if w.get("primary_topic") != None else None
                    for w in data ]
    domain_names = [w.get("primary_topic").get('domain').get('display_name')
                    if w.get("primary_topic") != None else None
                    for w in data ]

    df = pd.DataFrame({
        "title": titles,
        "domain_id": domain_ids,
        "domain_name": domain_names
    })
    return df

def is_english(text: str, detector) -> bool:
    if text is None:
        return False

    prob_eng = detector.compute_language_confidence(text, Language.ENGLISH)
    return prob_eng >= 0.65

def normalize_title(title: str):
    '''
    텍스트 정규화 함수
    '''
    title = title.lower()
    title = re.sub(r"[^a-z0-9\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title

ACADEMIC_STOPWORDS = {
    "base", "study","analysis","model","models","method","approach","framework","review",
    "investigation","evaluation","experiment","result","effect","impact",
    "application","research","performance","system","systems","based","data","algorithm","novel","paper",
    "case", "theory", "optimization", "approach", 
    "learning", "using", "management", "effects", "education", "role", "high",
    "development", "time", "sub", "sectional", 'cross'
}

def extract_top_keywords(titles, top_k=20):
    # 논문 제목 정규화
    titles_clean = [normalize_title(t) for t in titles]

    # TF-IDF Vectorizer
    vectorizer = TfidfVectorizer(
        stop_words="english",
        ngram_range=(2, 3),       # unigram + bigram + trigram
        min_df=2,                 # 2번 이상 등장한 n-gram만
        max_df=0.5,               # 60% 이상 문서에 나오면 제거
    )

    X = vectorizer.fit_transform(titles_clean)
    tfidf_scores = X.sum(axis=0)

    scores = [
        (word, tfidf_scores[0, idx])
        for word, idx in vectorizer.vocabulary_.items()
    ]

    # TF-IDF 상위순으로 정렬
    scores_sorted = sorted(scores, key=lambda x: x[1], reverse=True)
    # print(scores_sorted[:10])

    # 학술 불용어 제거
    result = []
    for word, score in scores_sorted:
        tokens = word.split()
        if any(t in ACADEMIC_STOPWORDS for t in tokens):
            continue
        result.append((word, score))

        if len(result) >= top_k:
            break

    return result

@task
def get_title_based_works(base_path, bucket, aws_conn_id="aws_s3_conn_id", **context):
    # 적재된 json 파일들 load 및 dataframe으로 변환
    prefix = f"{base_path}/{context['ds']}/"
    all_data = s3_utils.load_all_jsons(bucket, prefix, aws_conn_id=aws_conn_id)
    df = get_df_4_title_keyword(all_data)
    
    # 1. 영문 title 인지
    logging.info(f"[시작] 영문 Title 확인 ")
    detector = LanguageDetectorBuilder.from_all_spoken_languages().build()
    df['is_english'] = df['title'].apply(lambda x: is_english(x, detector))
    eng_df = df[df['is_english']].reset_index(drop=True)
    logging.info(f"[종료] 영문 Title 확인 ")
    
    # 2. TF-IDF 기준 빈도수 높은 키워드 5개 가져오기
    logging.info(f"[시작] TF-IDF 기준 빈도수 높은 키워드 수집")
    eng_titles = eng_df['title'].to_list()
    keywords   = extract_top_keywords(eng_titles, top_k=5)
    logging.info(f"[종료] TF-IDF 기준 빈도수 높은 키워드 수집")
    
    # 3. random keyword 선정 & 추천 Work 선택
    logging.info(f"[시작] Keyword 랜덤 선정 및 Work 선택")
    random.seed(1)
    fin_keyword = random.choice(keywords)[0]
    query = Works().search_filter(title=fin_keyword).sort(fwci="desc").get(per_page=10)
    fin_work = random.choice(query)
    logging.info(f"[종료] Keyword 랜덤 선정={fin_keyword} 및 Work 선택={fin_work.get('display_name')}")
    
    # 4. 사용할 필드 처리
    return [{  
        "keyword" : fin_keyword,
        "work_id" : fin_work.get("id").split("/")[-1],
        "domain_id":fin_work.get("primary_topic", {}).get('domain').get('id'),
        "domain_name":fin_work.get("primary_topic", {}).get('domain').get('display_name'),
        "title":fin_work.get("display_name"),
        "openalex_url" : fin_work.get("id"),
        "doi" : fin_work.get('doi'),
        "publication_date" : fin_work.get("publication_date"),
        "execution_date" : context['ds']
    },]
