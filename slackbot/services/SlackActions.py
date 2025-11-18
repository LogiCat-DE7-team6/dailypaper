import os
from datetime import datetime
import pytz
import requests
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

from slack_sdk import WebClient
from services.insert_user_selection import insert_user_selection
from services.select_daily_data import select_daily_data

from services.s3_services import S3Service
from utils.get_random_paper import get_random_paper

load_dotenv()
slack_token = os.getenv("SLACK_TOKEN")
bucket_name = os.getenv("BUCKET_NAME")
s3_access_key = os.getenv("S3_ACCESS_KEY")
s3_secret_key = os.getenv("S3_SECRET_KEY")

executor = ThreadPoolExecutor(max_workers=5)
client = WebClient(token=slack_token)

timezone = pytz.timezone("Asia/Seoul")
date = str(datetime.now(timezone))[0:10]

s3_credential = {
    'bucket_name': bucket_name,
    's3_access_key': s3_access_key,
    's3_secret_key': s3_secret_key
}

image_url = S3Service(**s3_credential).generate_presigned_url(f"{date}.png")

# 비동기 처리의 실패 여부 확인 함수 : 비동기 작업의 결과 확인 (결과가 없거나 예외가 있으면 raise 됨)
def on_task_complete(future):
    try:
        result = future.result()
        print(f"Task completed successfully: {result}")
    except Exception as e:
        # 실패 시 예외를 다시 던짐
        print(f"Task failed with error: {e}")
        raise e

class SlackActions():
    def __init__(self):
        pass

    # 명령어 입력시 버튼 띄우는 메소드
    def send_info_data():
        try:
            data = select_daily_data()
            data = data[0]
            inform_text = f"""
            {date}의 데이터 정보입니다.
            총 논문 수: {data[1]}
            - Physical Sciences 논문 수: {data[2]}
            - Social Sciences 논문 수: {data[3]}
            - Health Sciences 논문 수: {data[4]}
            - Life Sciences 논문 수: {data[5]}
            - 미분류 도메인 논문 수: {data[6]}
            - 공개 논문 수: {data[7]}
            """
            #- 비공개 논문 수: {data[8]}
            

            client.chat_postMessage(
                    channel = '#test',  # 채널 ID나 이름
                    text = '아래 버튼을 클릭해보세요!',  # 메시지 텍스트
                    blocks = [
                        {
                            'type': 'section',
                            'text': {
                                'type': 'mrkdwn',
                                'text': f'{inform_text}\n⬇️아래 버튼을 클릭하세요!'
                            }
                        },
                        {
                            'type': 'actions',
                            'elements': [
                                {
                                    'type': 'button',
                                    'text': {
                                        'type': 'plain_text',
                                        'text': '타입별 보기'
                                    },
                                    'action_id': 'button_click_1',  # 이 ID는 나중에 상호작용을 처리하는 데 사용
                                    'value': 'type'  # 클릭 시 전달될 값
                                },
                                {
                                    'type': 'button',
                                    'text': {
                                        'type': 'plain_text',
                                        'text': '주제별 보기'
                                    },
                                    'action_id': 'button_click_2',  # 이 ID는 나중에 상호작용을 처리하는 데 사용
                                    'value': 'title'  # 클릭 시 전달될 값
                                },
                                {
                                    'type': 'button',
                                    'text': {
                                        'type': 'plain_text',
                                        'text': '논문 추천 받기'
                                    },
                                    'action_id': 'button_click_3',  # 이 ID는 나중에 상호작용을 처리하는 데 사용
                                    'value': 'daily'  # 클릭 시 전달될 값
                                },
                                {
                                    'type': 'button',
                                    'text': {
                                        'type': 'plain_text',
                                        'text': '랜덤 추천 받기'
                                    },
                                    'action_id': 'button_click_4',  # 이 ID는 나중에 상호작용을 처리하는 데 사용
                                    'value': 'random'  # 클릭 시 전달될 값
                                }
                            ]
                        }
                    ]
            )

            return "", 200
        except Exception as e:
            raise e

    # 사용자 입력데이터에 대한 응답 메소드
    def send_user_select_result(payload, button_value=None):
        try:
            # 예: 모달에서 제출된 값 가져오기
            user = payload["user"]["username"]

            if button_value == 'random':
                data = get_random_paper()
                result_text = f"""
                {date} 랜덤 추천입니다.
                논문제목: {data.get('display_name') if len(data.get('display_name')) < 50 else data.get('display_name')[:50] + '...'}
                1저자 : {data.get('1st_author')}
                발행일자: {data.get('publication_date')}
                인용수: {data.get('cited_by_count')}
                논문타입: {data.get('type')}
                논문분류: {data.get('domain')}
                세부분류: {data.get('subfield')}
                URL: {data.get('oa_url')}
                """
            else:
                result_text = button_value
                pass
            client.chat_postMessage(
                channel="#test",
                text=f"✅",
                blocks=[
                    {
                        "type": "image",
                        "image_url": image_url,
                        "alt_text": "selected option image"
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"""✅ {user}님\n{result_text}"""
                        }
                    }
                ]
            )
            # user data 비동기 처리
            future = executor.submit(insert_user_selection, user, button_value)
            future.add_done_callback(on_task_complete)

            return "", 200
        except Exception as e:
            raise e