import os
from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone
import datetime
import requests, datetime
from dotenv import load_dotenv
from utils.slackbot_logger import get_logger

load_dotenv()
slack_token = os.getenv("SLACK_TOKEN")
channel_id = os.getenv("CHANNEL_ID")

def send_slack_alert():
    try:
        message = {
            "channel": channel_id,
            "text": f"🚨 Job 결과 ({datetime.datetime.now(timezone('Asia/Seoul'))})",
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "🚨 Scheduled Job 결과"}
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "✅확인!"},
                            "action_id": "open_modal_action",
                            "value": "test_job"
                        }
                    ]
                }
            ]
        }
        requests.post(
            "https://slack.com/api/chat.postMessage", 
            headers={
                "Authorization": f"Bearer {slack_token}",
                "Content-Type": "application/json"
            },
            json=message
        )
        print("Slack 자동 알림 전송 완료 ✅")
    except Exception as e:
        get_logger().exception(f"Exception occurred: {str(e)}")

# 스케줄러 설정
def cron_scheduler():
    scheduler = BackgroundScheduler(timezone=timezone('Asia/Seoul'))
    scheduler.add_job(send_slack_alert, 'cron', hour=15, minute=30)  # 매일 15시 30분 실행
    scheduler.start()
