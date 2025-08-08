# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 14:47:07 2023

@author: UM98XA
"""

from datetime import datetime
import os
import sys
import traceback
import win32com.client
import winreg

################################################################################################################################################################
# Functions
################################################################################################################################################################
# Function to notify myself with email if a script in Task Scheduler has failed to run
def EmailTaskFailure(task_name, attach_list = []):
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
    mail.Subject = 'Scheduled Task Failed - {}'.format(task_name)
    mail.HTMLBody = '<h3>Scheduled Task Failed: {} </h3><br/> Please look at the attached files for more information (Error-log and Output-log)'.format(task_name) #this field is optional
    
    # Attach files to email
    for attachment in attach_list:
        mail.Attachments.Add(attachment)
    
    # Send Email
    mail.Send()
    return

################################################################################################################################################################
# Main
################################################################################################################################################################
# Set path
project_path = os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\Fraud Dashboard\Google News Scraper")
os.chdir(project_path)

# Redirect output streams to txt file (prints, errors etc.)
sys.stdout = open('stdout.txt', 'w')
sys.stderr = open('stderr.txt', 'w')

# Print start time
print('\n' + 100*'-')
print('\nStart Time & Date MAIN: ' + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
print('\n')

try:
    # Run Script: Get (Daily) Exports for first 5 days of month
    from Scraper_GoogleNews import export_googlenews
    current_run_index = export_googlenews()
    
    # Run Script: move exported data
    if current_run_index == 5:
        # Upload latest file / Move
        print(f"Last run time of month: Move data ({current_run_index})")
        from Move_Data import move_googlenews_data
        move_googlenews_data()
        
    else:
        print(f"Don't move ({current_run_index})")
        
        
    ## Print Success ##
    print("\n Scraper GoogleNews Succesfull")
    
    # Print end time
    print(100*'-')
    print('\nEnd Time & Date: ' + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    
    # Close log files
    sys.stdout.close()
    sys.stderr.close() 

except Exception as e:
    ## Print Failure ##
    # Print errors and output to files and notify me through email
    print("\n Scraper GoogleNews Failed")
    print(e)
    print(traceback.format_exc(), file=sys.stderr)
    print(e, file=sys.stderr)
    
    # Print end time
    print(100*'-')
    print('\nEnd Time & Date: ' + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    
    # Close log files
    sys.stdout.close()
    sys.stderr.close() 
    
    # 'Notify me' script (send email)
    EmailTaskFailure('GoogleNewsScraper', [os.path.join(project_path, 'stderr.txt'),os.path.join(project_path, 'stdout.txt')])
    

    
