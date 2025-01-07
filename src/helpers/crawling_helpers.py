from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def init_chrome_options():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return chrome_options


def fetch_cycling_files(url: str, driver_path: str):
    logger.info(f"Opening URL: {url}")
    
    results = list()
    level_2_found = False
    try:
        chrome_options = init_chrome_options()
        remote_webdriver = 'remote_chromedriver'
        logger.info(f"Driver path: {remote_webdriver}:4444/wd/hub")
        with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options) as driver:
            driver.get(url)
            time.sleep(3)

            tbody = driver.find_element(By.ID, "tbody-content")
            rows = tbody.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                data_level = row.get_attribute("data-level")
                if data_level == "2":
                    level_2_found = True
                    continue
                elif level_2_found:
                    if data_level == "3":
                        cells = row.find_elements(By.TAG_NAME, "td")
                        cell_contents = [cell.text.strip() for cell in cells]
                        if cell_contents[3] == "CSV file":
                            results.append(
                                {
                                    "name": cell_contents[0],
                                    "last_modified": cell_contents[1],
                                    "size": cell_contents[2],
                                    "type": cell_contents[3],
                                    "description": cell_contents[4],
                                }
                            )
                            # logger.info(cell_contents)
                    else:
                        level_2_found = False
                        break
        logger.info(f"Successfully fetched {len(results)} results.")
        return results
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return []


# if __name__ == "__main__":
#     url = "https://cycling.data.tfl.gov.uk/"  # Replace with your actual URL
#     driver_path = "http://127.0.0.1:4444/wd/hub"
#     results = fetch_cycling_files(url, driver_path)
#     for result in results:
#         logger.info(result)