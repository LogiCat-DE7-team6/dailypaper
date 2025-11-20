# dailypaper

## 프로젝트 개요

이 프로젝트는 [OpenAlex API](https://docs.openalex.org/how-to-use-the-api/api-overview)를 활용하여 논문 추천 시스템을 구축하고, 이를 슬랙과 연동하여 매일 자동으로 업데이트된 논문 추천을 공유하는 시스템을 만드는 것을 목표로 한다. 이 시스템은 사용자들에게 최신 연구 동향을 제공하고, 사용자에게 논문 추천을 슬랙 메시지로 전달하는 것을 목표로 한다.

프로젝트 기간 : 2025.11.14 ~ 2025.11.20

#### 프로젝트 목표
1. OpenAlex API를 활용해 논문 메타데이터(제목, 저자, 발행일 등)를 자동으로 수집하고 분석하여 전송하고, 수집된 데이터를 이용하여 논문을 추천한다.
2. 슬랙에서 ‘/’ 커맨드를 이용하여 수집된 논문의 메타 정보를 받아 볼 수 있으며, 이를 통해 사용자들이 최신 논문을 쉽게 접근하고, 최신 연구 동향을 파악할 수 있도록 돕는다.
3. 슬랙api를 활용하여 사용자가 옵션을 선택하면, 해당 옵션의 기능을 제공한다.

&nbsp;

&nbsp;

## 활용 기술 및 프레임 워크 
<img width="1575" height="838" alt="image" src="https://github.com/user-attachments/assets/58459816-6d29-404e-affb-61ab413579f2" />

&nbsp;

&nbsp;

## 프로젝트 흐름도
<img width="1642" height="987" alt="image" src="https://github.com/user-attachments/assets/41ab4ad5-e389-47a1-9869-89f560fa88dc" />

- Apache Airflow DAG를 통해 OpenAlex API 및 Pyalex 라이브러리를 활용한 데이터를 자동으로 수집하고, Snowflake에 저장·정제하는 파이프라인을 구성
- Snowflake에 적재·정제된 데이터를 기반으로 Superset에서 시각화 대시보드를 구축 후 DAG를 통해 매일 이미지 형태로 S3에 저장
- 사용자가 Slack 봇을 호출하면, Flask를 통해 요청을 처리하여 Snowflake DB와 S3의 최신 데이터 조회 후 요청 유형에 맞는 정보를 메시지 형태로 자동 응답

&nbsp;

&nbsp;

## 프로젝트 폴더 구조
```
├─airflow/
│  ├─config/
│  ├─dags/
│  │  ├─scripts/
│  │  └─utils/
│  ├─data/
│  └─plugins/
└─slackbot
    ├─controller/
    ├─services/
    └─utils/
```

&nbsp;

&nbsp;

## 프로젝트 결과

(슬랙봇 호출 이미지)
