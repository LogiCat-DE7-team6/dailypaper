from flask import Flask
from controller.route import bp
from utils.send_alert import cron_scheduler
from utils.slackbot_logger import get_logger
import threading

app = Flask(__name__)

app.register_blueprint(bp)
send_alert = cron_scheduler()
# 크론에 따른 send message

@app.errorhandler(Exception)
def handle_exception(e):
    logger = get_logger()
    logger.exception(f"Exception occurred: {str(e)}")
    return str(e), 500

@app.route("/")
def home():
    return "Hello, World!"

if __name__ == '__main__':
    scheduler_thread = threading.Thread(target=send_alert)
    scheduler_thread.daemon = True
    app.run(host='0.0.0.0', debug=True, port=5000, threaded=True)