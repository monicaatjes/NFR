# -*- coding: utf-8 -*-
"""
Get ORX news CSV from website using Selenium Chrome driver
  
@author: UM98XA
"""

import shutil
import os 
import glob
import time
import json 
import chromedriver_autoinstaller_fix
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from subprocess import CREATE_NO_WINDOW
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 

# Set Paths
project_path = os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\ORX News Top 5 Report")
download_path = os.path.join(r"C:\Users", os.environ['UserName'], r"OneDrive - ING\Downloads")  
credentials_path = os.path.join(r"C:\Users", os.environ['UserName'], r"OneDrive - ING\Documents\Python Projects\Credentials")
active_chromedriver_path = chromedriver_autoinstaller_fix.install(path=os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\Documentation\Chromedriver"))

# Print start time
print(100*'-')
print('# Scraper_ORX Start Time & Date: ' + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

################################################################################################################################################################
# Functions
################################################################################################################################################################
def date_diff_months(start_date, end_date):
    """ Returns number of months between the months of two (datetime) dates as whole integer """
    diff = (end_date.year - start_date.year) * 12 + (end_date.month  - start_date.month)
    return diff

def clean_archive(archive_path, file_name, data_retention_period):
    """
        archive_path (str)          = Path-location of archive folder
        data_retention_period (int) = Number of months the file is kept for, older files are removed
    """
    
    # Deletes files older than a certain period in archive folder
    archive_files = os.listdir(archive_path)
        
    # Using Date Modified Timestamps
    archive_files = [os.path.join(archive_path, file) for file in archive_files if file_name in file]
    archive_files = [[datetime.fromtimestamp(os.path.getmtime(file_path)), file_path] for file_path in archive_files] 
    
    # Get files to delete and remove them
    delete_files = [file[1] for file in archive_files if date_diff_months(file[0], datetime.now()) > data_retention_period]
    for file in delete_files:
        os.remove(file)
    return
    
def archive_files(folder_path, file_name):
    """ Moves file to archive folder within the supplied folder """
    # Create archive if not existent
    archive_path = os.path.join(folder_path, 'Archive')
    if not os.path.exists(archive_path):
        os.makedirs(archive_path)
        
    # Move file to archive
    [shutil.move(os.path.join(folder_path, file), os.path.join(archive_path, datetime.fromtimestamp(os.path.getmtime(os.path.join(folder_path, file))).strftime("%Y%m%d_") + file)) for file in os.listdir(folder_path) if file == file_name]     
    return 

def download_wait(directory, timeout=60, nfiles=None):
        """
        Wait for downloads to finish with a specified timeout.
    
        Args
        ----
        directory : str
            The path to the folder where the files will be downloaded.
        timeout : int
            How many seconds to wait until timing out.
        nfiles : int, defaults to None
            If provided, also wait for the expected number of files.
    
        """
        # Wait till download started
        seconds = 0
        download_start = False
        while not download_start and seconds < timeout/2:
            download_start = False
            files = os.listdir(directory)
            for fname in files:
                if fname.endswith('.crdownload'):
                    download_start = True
            
            # Sleep untill next iteration
            time.sleep(1)
            seconds += 1        
        
        # Wait till download finished
        seconds = 0
        download_finish = False
        while not download_finish and seconds < timeout:
            download_finish = True
            files = os.listdir(directory)
            if nfiles and len(files) != nfiles:
                download_finish = False
    
            for fname in files:
                if fname.endswith('.crdownload'):
                    download_finish = False
            
            # Sleep untill next iteration
            time.sleep(1)
            seconds += 1
        return None

################################################################################################################################################################
# ORX Data Download Function
################################################################################################################################################################
def refresh_orx(driver, project_path, credentials_path, download_path):
    """ Refreshes ORX source file inside project folder """

    # Archive & Remove old files
    archive_files(folder_path = os.path.join(project_path, 'Data Exports'), file_name = 'ORX export.csv')
    clean_archive(archive_path = os.path.join(project_path, 'Data Exports', 'Archive'), file_name = 'ORX export.csv', data_retention_period = 2)
    
    # Get Credentials for ORX site
    with open(os.path.join(credentials_path, "ORX_credentials.json"), "r") as file:
        creds = json.load(file)
    
    # Open ORX Site
    print('- Opening ORX URL')
    driver.get("https://sso.orx.org/")
    time.sleep(2)
    
    # Press 'Accept Cookies' Button
    startTime = datetime.now()
    i = 1
    while True:
        print('- Pressing Accept Cookies button')
        try:
            driver.find_element_by_xpath("//button[@id='hs-eu-confirmation-button']").click()
            time.sleep(1)
            break
        except Exception as e:
            print(e)
            time.sleep(1)
        if (datetime.now() - startTime).total_seconds() > 20*i:
            driver.get("https://sso.orx.org/")
            i += 1
        elif (datetime.now() - startTime).total_seconds() > 300:
            break
    del i, startTime
    
    # Login to ORX site
    print('- Logging In')
    driver.find_element_by_xpath("//a[@class='header__button button   ']").click()
    time.sleep(2)
    user_box = driver.find_element_by_id('1-email')
    user_box.send_keys(creds['user'])
    passw_box = driver.find_element_by_id('1-password')
    passw_box.send_keys(creds['password'])
    driver.find_element_by_xpath("//button[@class='auth0-lock-submit']").click()
    
    # Navigate to ORX news page
    time.sleep(1)
    driver.get("https://news.orx.org/search/news/")
    time.sleep(1)
    
    # # Press another cookies ??
    # startTime = datetime.now()
    # i = 1
    # while True:
    #     print('- Pressing Accept Cookies button')
    #     try:
    #         driver.find_element_by_xpath("//button[@id='ccc-notify-accept']").click()
    #         time.sleep(1)
    #         break
    #     except Exception as e:
    #         print(e)
    #         time.sleep(1)
    #     if (datetime.now() - startTime).total_seconds() > 20*i:
    #         driver.get("https://news.orx.org/search/news/")
    #         i += 1
    #     elif (datetime.now() - startTime).total_seconds() > 300:
    #         break
    # del i, startTime
    
    # Download CSV file
    print('- Downloading File')
    driver.get("https://news.orx.org/news/csv?")
    
    # Wait till download finishes
    download_wait(download_path, 60)
        
    # Copy File from downloads to project folder
    print('- Copying and moving file')
    list_of_files = glob.glob(os.path.join(download_path, '*export.csv')) # * means all if need specific format then *.csv
    shutil.move(max(list_of_files, key=os.path.getctime), os.path.join(project_path,"Data Exports", "ORX export.csv"))

################################################################################################################################################################
# Main
################################################################################################################################################################
print('- Opening Chromedriver')
chrome_options = webdriver.ChromeOptions()
chrome_options = Options()
chrome_options.add_argument("--headless")                              # Don't show browser
chrome_options.add_argument("--no-sandbox")                            # Runs scraper in serverless mode (e.g. in Docker or Kubernetes Containers)
chrome_options.add_argument("--disable-dev-shm-usage")                 # Runs scraper in serverless mode (e.g. in Docker or Kubernetes Containers)
chrome_options.add_argument("--disable-search-engine-choice-screen")
chrome_prefs = {"download.default_directory": download_path, 'download.prompt_for_download': False}           # Set download folder (windows fix for headless)
chrome_options.experimental_options["prefs"] = chrome_prefs
service = Service(active_chromedriver_path)
service.creationflags = CREATE_NO_WINDOW                             # Set Incognito 
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.maximize_window()

# Run ORX Scraper
try:
    refresh_orx(driver, project_path, credentials_path, download_path)
    
except Exception as error:
    print("- Scraper failed please check:")
    print(error)
    
finally:
    print('- Closing Chromedriver')
    try:
        driver.close()
        driver.quit()
    except:
        driver = None

    # Print end time
    print('# Scraper_ORX End Time & Date: ' + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    print(100*'-')