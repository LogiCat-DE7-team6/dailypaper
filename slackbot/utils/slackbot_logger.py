import logging
from logging.handlers import RotatingFileHandler

# 로깅 설정을 위한 함수
def get_logger(name="slack_bot_logger", log_file="./app.log"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # 파일 로깅: 로그 파일 크기 제한 및 백업
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 콘솔 출력: 개발 환경에서 콘솔에도 로깅
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger