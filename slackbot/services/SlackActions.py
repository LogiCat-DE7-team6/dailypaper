import os
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

from slack_sdk import WebClient
from services.insert_user_selection import insert_user_selection
from services.get_data import get_data

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
            data = get_data("main")
            inform_text = f"""
:books: {data[0]} 기준 현재 보유 중인 논문은 총 {data[1]}개이며, 전일 대비 {data[2]}개 증가했습니다.
또한 최근 3일 동안 가장 많이 추가된 분야는 {data[3]} - {data[4]}로, 총 {data[5]}개가 새로 등록되었습니다.
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
                                    'value': 'field'  # 클릭 시 전달될 값
                                },
                                {
                                    'type': 'button',
                                    'text': {
                                        'type': 'plain_text',
                                        'text': '논문 추천 받기'
                                    },
                                    'action_id': 'button_click_3',  # 이 ID는 나중에 상호작용을 처리하는 데 사용
                                    'value': 'recommend'  # 클릭 시 전달될 값
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
                {date} 랜덤 추천입니다.\n
                논문제목: {data.get('display_name') if len(data.get('display_name')) < 50 else data.get('display_name')[:50] + '...'}\n
                1저자 : {data.get('1st_author')}\n
                발행일자: {data.get('publication_date')}\n
                인용수: {data.get('cited_by_count')}\n
                논문타입: {data.get('type')}\n
                논문분류: {data.get('domain')}\n
                세부분류: {data.get('subfield')}\n
                OpenAlex-URL: {data.get('openalex_url')}\n
                """             
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"""✅ {user}님\n{result_text}"""
                        }
                    }
                ]

            elif button_value == 'recommend':
                data = get_data(button_value)

                result_text = f'''
                **오늘의 추천 논문**\n
                논문 수집 기간: {data[0] - timedelta(6)} ~ {data[0]}\n
                많이 인용된 논문\n
                - {(data[1].split(','))[0]}\n
                - {(data[1].split(','))[1]}\n
                - {(data[1].split(','))[2]}\n
                오늘의 keyword = "{data[2]}"\n
                - URL: {data[3]}\n
                주목할만한 논문
                '''
                urls = data[4].split(',')
                for url in urls:
                    result_text += f'- {url}\n                 '

                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"""✅ {user}님\n{result_text}"""
                        }
                    }
                ]    

            elif button_value == 'field':
                data = get_data(button_value)
                result_text = f'''
                :gear: Physical Sciences: {data[0]}\n
                :microscope: Life Sciences: {data[1]}\n
                :hospital: Health Sciences: {data[2]}\n
                :brain: Social Sciences: {data[3]}\n
                :question: Unknown: {data[4]}
                '''             
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

            elif button_value == 'type':
                data = get_data(button_value)
                total = sum(data)
                result_text = f'''
                :card_index_dividers: 논문 유형별 보유 현황을 안내드립니다.\n

                - Article: {data[0]}개 ({data[0]/total*100:.1f}%)\n
                - Book Chapter: {data[1]}개 ({data[1]/total*100:.1f}%)\n
                - Dataset: {data[2]}개 ({data[2]/total*100:.1f}%)\n
                - Preprint: {data[3]}개 ({data[3]/total*100:.1f}%)\n
                - Dissertation: {data[4]}개 ({data[4]/total*100:.1f}%)\n
                - Book: {data[5]}개 ({data[5]/total*100:.1f}%)\n
                - Review: {data[6]}개 ({data[6]/total*100:.1f}%)\n
                - Paratext: {data[7]}개 ({data[7]/total*100:.1f}%)\n
                - Others: {data[8]}개 ({data[8]/total*100:.1f}%)
                '''

                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"""✅ {user}님\n{result_text}"""
                        }
                    }
                ]

            client.chat_postMessage(
                channel="#test",
                text=f"✅",
                blocks=blocks
            )
            # user data 비동기 처리
            future = executor.submit(insert_user_selection, user, button_value)
            future.add_done_callback(on_task_complete)

            return "", 200
        except Exception as e:
            raise e