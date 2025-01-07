from selenium import webdriver
from selenium.webdriver.common.by import By
import time

options = webdriver.ChromeOptions()
options.add_argument("--headless")  # example
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Remote("http://127.0.0.1:4444/wd/hub", options=options)

# Your selenium code

driver.get("https://cycling.data.tfl.gov.uk/")
time.sleep(3)

results = list()
level_2_found = False
try:
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
                    print(cell_contents)
            else:
                level_2_found = False
                break
    print(f"Successfully fetched {len(results)} results.")

except Exception as e:
    print(f"Error fetching data: {e}")
