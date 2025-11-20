# dailypaper

## 📝 프로젝트 개요

이 프로젝트는 [OpenAlex API](https://docs.openalex.org/how-to-use-the-api/api-overview)를 활용하여 논문 추천 시스템을 구축하고, 이를 슬랙과 연동하여 매일 자동으로 업데이트된 논문 추천을 공유하는 시스템을 만드는 것을 목표로 한다. 이 시스템은 사용자들에게 최신 연구 동향을 제공하고, 사용자에게 논문 추천을 슬랙 메시지로 전달하는 것을 목표로 한다.

#### 프로젝트 목표
1. OpenAlex API를 활용해 논문 메타데이터(제목, 저자, 발행일 등)를 자동으로 수집하고 분석하여 전송하고, 수집된 데이터를 이용하여 논문을 추천한다.
2. 슬랙에서 ‘/’ 커맨드를 이용하여 수집된 논문의 메타 정보를 받아 볼 수 있으며, 이를 통해 사용자들이 최신 논문을 쉽게 접근하고, 최신 연구 동향을 파악할 수 있도록 돕는다.
3. 슬랙api를 활용하여 사용자가 옵션을 선택하면, 해당 옵션의 기능을 제공한다.

#### 프로젝트 기간
2025.11.14 ~ 2025.11.20
&nbsp;

&nbsp;

## 🛠 활용 기술 및 프레임 워크 
<img width="1575" height="838" alt="image" src="https://github.com/user-attachments/assets/58459816-6d29-404e-affb-61ab413579f2" />

&nbsp;

&nbsp;

## 🔄 프로젝트 흐름도
<img width="1642" height="987" alt="image" src="https://github.com/user-attachments/assets/41ab4ad5-e389-47a1-9869-89f560fa88dc" />

&nbsp;

- Apache Airflow DAG를 통해 OpenAlex API 및 Pyalex 라이브러리를 활용한 데이터를 자동으로 수집하고, Snowflake에 저장·정제하는 파이프라인을 구성
- Snowflake에 적재·정제된 데이터를 기반으로 Superset에서 시각화 대시보드를 구축 후 DAG를 통해 매일 이미지 형태로 S3에 저장
- 사용자가 Slack 봇을 호출하면, Flask를 통해 요청을 처리하여 Snowflake DB와 S3의 최신 데이터 조회 후 요청 유형에 맞는 정보를 메시지 형태로 자동 응답

&nbsp;

&nbsp;

## 📁 프로젝트 폴더 구조
```
dailypaper/
├── airflow/                      # Airflow 워크플로우 코드
│   ├── config/                   # Airflow 설정 파일
│   ├── dags/                     # DAG 정의 스크립트
│   │   ├── scripts/              # DAG에서 사용하는 스크립트 함수
│   │   └── utils/                # DAG에서 공통으로 사용하는 유틸리티 함수
│   ├── data/                     # Airflow에서 사용하는 중간 데이터 저장소
│   ├── plugins/                  # 커스텀 Airflow 플러그인
│   ├── variables.json            # Airflow 사용 변수
│   ├── Dockerfile.airflow
│   ├── docker-compose.yaml
│   └── requirements.txt 
│
├── slackbot/                     # 슬랙 봇 서버 코드
│   ├── controller/               # 슬랙 명령/이벤트 처리 로직
│   ├── services/                 # DB 조회, API 통신 등의 서비스 계층
│   ├── utils/                    # 공통 유틸리티 함수
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
│
├── superset/                     # Superset 관련 구성
│   ├── docker                    # requirements-local.txt 위치한 폴더
│   ├── dashboard….zip            # Superset 대시보드 설정 파일
│   ├── Dockerfile        
│   └──  docker-compose-non-dev.yml
│  
└── README.md                     # 프로젝트 설명 파일
```

&nbsp;

&nbsp;

## 📊 프로젝트 결과

### 1. dailypaper 봇 호출

호출 시점까지 수집된 데이터를 기준으로 **논문 보유 현황**을 대시보드와 요약글을 통해 제공한다.

<img alt="image" src="https://github.com/user-attachment:e6e5f34c-4da9-424e-9b76-5f18357d514b />

### 2. 타입별 보기

출판 형태나 성격을 나타내는 분류(Article, Preprint, Book 등)를 기준으로 출간된 학술 저작물의 현황을 볼 수 있다.

<img alt="image" src="https://github.com/user-attachment:077186a1-d58d-4661-880a-61a3918c4112 />

### 3. 주제별 보기

4개의 도메인(Physical Sciences, Life Sciences, Health Sciences, Social Sciences)에 대한 현황과 세부 카테고리별 비율을 대시보드로 제공한다.

<img alt="image" src="https://github.com/user-attachment:04ec1e0c-3ca5-4198-8a67-7d38c3e457c1 />

### 4. 논문 추천 받기

특정 기준별(인용이 많이된, 제목에 언급이 많이된, 최근 출간물 중 fwci 점수가 높은) 선정된 논문을 추천한다.
<img alt="image" src="https://github.com/user-attachment:b30eaed4-59e7-447a-b80b-594696c6b42f />

### 5. 랜덤 추천 받기

openalex에 등록된 논문 중 1건을 랜덤으로 선정하여 제공한다.

<img alt="image" src="https://github.com/user-attachment:1da7c6e5-d018-44af-b7a5-45ced455b469 />
