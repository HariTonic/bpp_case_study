from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os

class PVWattDataRead:

    def pv_watt_read_data(self):
        # Get the current script directory
        script_dir = os.getcwd()

        # Configure Chrome WebDriver to download files to the script directory
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": script_dir,  # Save in the script's directory
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        # Launch the browser
        driver = webdriver.Chrome(options=options)

        try:
            # Step 1: Open PVWatts website
            driver.get("https://pvwatts.nrel.gov/pvwatts.php")

            # Step 2: Locate search box and enter "Delhi"
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "myloc"))
            )
            search_box.clear()
            search_box.send_keys("Delhi")

            # Step 3: Click the "Go" button
            go_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "go"))
            )
            go_button.click()

            # Step 4: Wait for the results page and navigate using JavaScript
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            driver.execute_script("appNav('results', 'right');")

            # Step 5: Wait and Click on the Download CSV link
            download_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[@class='pvGevent' and @pvevent='downloadHourly']"))
            )
            download_button.click()

            # Step 6: Wait for the download to complete
            time.sleep(5)  # Adjust based on internet speed

            print(f"File downloaded to: {script_dir}")

        finally:
            driver.quit()
