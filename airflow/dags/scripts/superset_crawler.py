from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.remote_connection import RemoteConnection
import time
from PIL import Image
import logging
import os

logging.basicConfig(level=logging.INFO)

def crawl_superset_dashboard(url, id, pw, tab_dict):
    logging.info(f"크롤링 시작. 타겟 URL: {url}")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1980,1400")

    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        logging.info("Superset URL 접근 시도.")
        driver.get(url)
        
        # 페이지 로딩 및 로그인 필요 여부 확인
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "username"))
            )
            login_required = True
        except:
            login_required = False

        logging.info(f"로그인 필요 상태: {login_required}")

        if login_required:
            logging.info("로그인 정보 입력 및 클릭 시도.")
            driver.find_element(By.ID, "username").send_keys(id)
            driver.find_element(By.ID, "password").send_keys(pw)
            driver.find_element(By.CSS_SELECTOR, 'button[data-test="login-button"]').click()
            
            logging.info(f"로그인 후 현재 URL: {driver.current_url}")

            # 로그인 후 대시보드 로딩을 기다립니다.
            time.sleep(10)

        saved_paths = {}

        for tab_selector, output_path in tab_dict.items():
            logging.info(f"탭 '{tab_selector}'로 이동 시도. 저장 경로: {output_path}")
            
            # 탭 클릭
            tab = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, f"div[data-node-key='{tab_selector}']"))
            )
            tab.click()
            logging.info(f"탭 '{tab_selector}' 클릭 완료.")
            
            # 차트 렌더링을 위해 충분히 대기
            time.sleep(10)
            logging.info("차트 렌더링을 위해 10초 대기 완료.")

            # 스크린샷 저장
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            driver.save_screenshot(output_path)
            logging.info(f"전체 스크린샷 저장 완료: {output_path}")
            
            # 이미지 자르기 (크롭)
            img = Image.open(output_path)
            cropped_img = img.crop((55, 190, img.width-14, img.height-63))
            cropped_img.save(output_path)
            logging.info(f"스크린샷 크롭 완료 및 덮어쓰기 완료.")
            saved_paths[tab_selector] = output_path

    except Exception as e:
        logging.error(f"크롤링 중 오류 발생: {e}")
        # 오류 발생 시 디버깅을 위해 최종 화면 캡처 시도 (실패 시 무시)
        try:
            error_screenshot_path = os.path.join(os.path.dirname(output_path), "ZZZ_error_dump.png")
            driver.save_screenshot(error_screenshot_path)
            logging.error(f"오류 발생 시점 스크린샷 저장: {error_screenshot_path}")
        except:
            pass
        raise
        
    finally:
        logging.info("WebDriver 종료를 시도합니다.")
        driver.quit()

    logging.info("크롤링 작업 최종 성공.")

    return saved_paths
    