from flask import Flask
from controller.route import bp
from utils.send_alert import cron_scheduler
from utils.slackbot_logger import get_logger
import threading

app = Flask(__name__)

app.register_blueprint(bp)
# 크론 기능 - 시간에 대한 알림 설정
#send_alert = cron_scheduler()

@app.errorhandler(Exception)
def handle_exception(e):
    logger = get_logger()
    logger.exception(f"Exception occurred: {str(e)}")
    return str(e), 500

@app.route("/")
def home():
    return "Hello, World!"

if __name__ == '__main__':
    # 크론 기능을 위한 스케쥴러 쓰레드 생성. 없어도 돌아감.
    #scheduler_thread = threading.Thread(target=send_alert)
    #scheduler_thread.daemon = True
    app.run(host='0.0.0.0', debug=True, port=5000, threaded=True)