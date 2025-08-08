# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 14:47:28 2023

@author: UM98XA
"""

import shutil
import os
from datetime import datetime
import win32com.client
import winreg

################################################################################################################################################################
# Functions
################################################################################################################################################################
# Function to notify myself with email to notify that Google News has finished running
def EmailSuccess():
    """
    Sends email to yourself in case of a scheduled task failure
    Parameters
    ----------
    task_name: String
            Name of task that failed
    attach_list : list of File-Paths (eg. [C:\\Users\\...\\stderr.txt, ...]), optional
        List of the attachments that need to be included in the mail (Error + Log files). The default is [].

    Returns
    -------
    None.

    """
    # Connect to Outlook Application
    outlook = win32com.client.Dispatch('outlook.application')
    
    ## Get your emailaddress ##
    # Read Windows Registry to retrieve users Full Lastname
    try:
        key = winreg.OpenKeyEx(winreg.HKEY_CURRENT_USER, "Software\\Microsoft\\Office\\Common\\UserInfo")
        current_user = winreg.QueryValueEx(key,"UserName")[0]
        current_user = current_user.split(',')[0].lower()
    except:
        current_user = ""
        
    # Loop over accounts in outlook to find name that matches users name
    for i, account in enumerate(outlook.Session.Accounts): # Loops over all accounts logged into outlook 
        # Initialize
        if i == 0:
            myaccount = account.DisplayName
        
        # Get Emailadress if last-Name in registry aligns with name in email-account
        if current_user in account.DisplayName:
            myaccount = account.DisplayName
            break
   
    # Create and set email
    mail = outlook.CreateItem(0)
    mail.To = myaccount
    mail.Subject = 'Google News Scraper is ready'
    mail.HTMLBody = '<h3>Google News Scraper has finished running </h3><br/> Please have a look and refresh the Power BI dashboard manually'
    
    # Send Email
    mail.Send()
    return

################################################################################################################################################################
# Main
################################################################################################################################################################
def move_googlenews_data():
    # Get file path
    path_source = os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\Fraud Dashboard\Google News Scraper\Exports", datetime.now().strftime("%Y %b ") + "Fraud GoogleNews.xlsx")
    
    # Get Destination path
    path_dest = os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\Fraud Dashboard\Report Data\Fraud GoogleNews.xlsx")
    
    # Copy file
    shutil.copy(path_source, path_dest)
    
    # Notify That files ready for refresh
    EmailSuccess()
    
if __name__ == "__main__":
    move_googlenews_data()