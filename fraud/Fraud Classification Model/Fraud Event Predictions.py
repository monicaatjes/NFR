# -*- coding: utf-8 -*-
"""
Classify ORX News / Sas Oprisk events based on Fraud category

# To Do
- Compare old data with new

Created on Fri Oct 13 13:39:19 2023

@author: UM98XA
"""

from datetime import datetime
from datasets import Dataset
from datasets.utils.logging import set_verbosity_error
import pandas as pd
import pandas.io.formats.excel
import os 
import re
import warnings
import numpy

# BERT
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

# Remove warnings/logging
warnings.filterwarnings("ignore")
set_verbosity_error()

# Set PATH
project_path = os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\Fraud Dashboard\Fraud Classification Model")

print(100*'-')
print('# Preprocess Data Input Start Time & Date: ' + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
################################################################################################################################################################
# Functions
################################################################################################################################################################
# function: Clean NLP texts
def clean_text(text):
    # Clear Special UTF-8 Characters
    clean_text = " ".join(text.replace("_x000D_","").replace("\n", " ").replace("\xa0", " ").strip().split())
    
    # Clean constant texts
    clean_text = re.sub(r"Data supplied by ORX News.*\d{4}", "", clean_text)
    clean_text = re.sub("A DEEP DIVE IS NOW AVAILABLE FOR THIS LOSS EVENT ", "", clean_text) 
    return clean_text

# Get Classification Labels: Fraud Category L1
def get_probs(text_list, tokenizer, model):
    """Gets Predictions from the BERT Classification Model for a set of rows"""
    encoded_input = tokenizer(
        text_list, padding=True, truncation=True, max_length=512, return_tensors='pt'
    )
    encoded_input = {k: v for k, v in encoded_input.items()}
    with torch.no_grad():
        model_output = model(**encoded_input)
    prob = model_output.logits.sigmoid()
    return prob

def get_labels(prob, targets):
    prob = torch.FloatTensor(prob)
    prob = prob*(prob>=0.3) # Subset probs that are larger than 0.3 
    pred = (prob==prob.max(dim=1, keepdim=True)[0]).float().numpy()                 # Get Array (Boolean) 
    pred = ', '.join([lab for i, lab in zip(pred[0].tolist(), targets) if i == 1])  # Get Label as string
    pred = "No Fraud reporting category" if pred == "" else pred # If no predictions (e.g. all probs lower than 0.3)
    return pred

################################################################################################################################################################
# Load Bert Model
################################################################################################################################################################
print("- Load BERT Model")
# Model Fraud Labels #
checkpoint_model = "distilbert-base-uncased"
model = AutoModelForSequenceClassification.from_pretrained(os.path.join(project_path, "Classification Model", "Model - Fraud Classifier"))
model.eval()
tokenizer = AutoTokenizer.from_pretrained(checkpoint_model, do_lower_case=True)

# Set target Labels
targets = ["Cybercrime", 
           "Authorised Push Payment Fraud",
           "Card Fraud",
           "Internal (Occupational) Fraud",
           "Lending Fraud", 
           "No Fraud reporting category",
           "Other Fraud",
           "Transaction Fraud",
           "e-Banking Fraud"]

# Model SAS Headlines Summarizer # 
checkpoint_summary = "facebook/bart-large-cnn"
summarizer = pipeline("summarization", min_length = 10, max_length = 50, model=checkpoint_summary)

################################################################################################################################################################
# Load Data
################################################################################################################################################################
print("- Load Data (ORX & SAS)")
# Set columns to keep
keep_ORX = ['Story Reference Number', 'Headline', 'Digest Text', 
            'Scenario Category Name', 'Event Type Level 1 Name', 'Event Type Level 2 Name',
            'Business Line Level 1 Name', 'Business Line Level 2 Name',            
            'Region Name','Country Name',
            'Loss Amount (USD)', 'Publish Date', 'Institution Name']

keep_SAS = ['Reference ID Code', 'Description of Event',
            'Activity','Sub Risk Category','Event Risk Category',
            'Basel Business Line - Level 1', 'Basel Business Line - Level 2',
            'Region of Domicile','Country of Incident',
            'Loss Amount ($M)', 'Publish Date', 'Institution Name']

# Read data ORX and SAS
df_input_ORX = pd.read_excel(os.path.join(project_path, r"Data\Fraud Events.xlsx"), sheet_name= "ORX")
df_input_ORX = df_input_ORX[keep_ORX]
df_input_ORX['Source'] = "ORX"
df_input_SAS = pd.read_excel(os.path.join(project_path, r"Data\Fraud Events.xlsx"), sheet_name= "SAS")
df_input_SAS = df_input_SAS[keep_SAS]
df_input_SAS['Source'] = "SAS"

# Create ID columns + remove old
df_input_ORX['ID'] = df_input_ORX['Source'] + "_" + df_input_ORX['Story Reference Number'].astype(str)
df_input_SAS['ID'] = df_input_SAS['Source'] + "_" + df_input_SAS['Reference ID Code'].astype(str)
del df_input_ORX['Story Reference Number'], df_input_SAS['Reference ID Code']

# Filter old and new data !!! => Drop old events not in new data first
df_previous_output = pd.read_excel(os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\Fraud Dashboard\Report Data\External Fraud Events.xlsx"))
df_input_ORX = df_input_ORX[~df_input_ORX['ID'].isin(df_previous_output['ID'])]
df_input_SAS = df_input_SAS[~df_input_SAS['ID'].isin(df_previous_output['ID'])]

################################################################################################################################################################
# Prep Data
################################################################################################################################################################
print("- Prep Data")
# Combine/Clean NLP Columns
nlp_col_ORX = ['Headline', 'Digest Text']
nlp_col_SAS = ['Description of Event']
for df, nlp_col in zip([df_input_ORX, df_input_SAS], [nlp_col_ORX, nlp_col_SAS]):
    # Combine
    df['NLP_Text'] = ''
    for column in nlp_col:
        df['NLP_Text'] = df['NLP_Text'] + ' ' + df[column]
    
    # Clean 
    df['NLP_Text'] = df['NLP_Text'].apply(lambda text: clean_text(text))
del df, nlp_col, column

# Clean Loss Columns
df_input_ORX['Loss Amount (USD)'] = pd.to_numeric(df_input_ORX['Loss Amount (USD)'], errors='coerce').fillna(0)
df_input_SAS['Loss Amount ($M)'] = df_input_SAS['Loss Amount ($M)']*(10**6)

# Rename columns to align datasets
df_input_ORX.rename(columns={'Scenario Category Name':'Scenario','Event Type Level 1 Name':'Event_Type_L1','Event Type Level 2 Name':'Event_Type_L2',
                             'Business Line Level 1 Name':'Business_Line_L1','Business Line Level 2 Name':'Business_Line_L2',
                             'Region Name':'Region','Country Name':'Country', 'Loss Amount (USD)':'Loss_USD', 'Digest Text':'Event Description'}, inplace=True)

df_input_SAS.rename(columns={'Activity':'Scenario','Event Risk Category':'Event_Type_L1','Sub Risk Category':'Event_Type_L2',
                             'Basel Business Line - Level 1':'Business_Line_L1','Basel Business Line - Level 2':'Business_Line_L2',
                             'Region of Domicile':'Region','Country of Incident':'Country', 'Loss Amount ($M)':'Loss_USD','Description of Event':'Event Description'}, inplace=True)

# Add Headline for SAS (only new rows)
if df_input_SAS.shape[0]:
    df_input_SAS['Headline'] = df_input_SAS['Event Description'].apply(lambda x: summarizer(x, truncation=True)[0]['summary_text'].strip().replace(' .', '.'))

# Append Dataframes
df_input = pd.concat([df_input_ORX, df_input_SAS])
del df_input_ORX, df_input_SAS

# Set ID column to index
df_input.set_index('ID', inplace=True)

# Align categories if necessary (Scenario / Event Type / Business Line / Region / Country)
df_input['Scenario'].replace('Embezzlement ', 'Embezzlement', inplace=True)
df_input['Scenario'].replace('Insider trading', 'Insider Trading', inplace=True)
df_input['Event_Type_L2'].replace('External Theft & Fraud', 'Theft and Fraud', inplace=True)
df_input['Event_Type_L2'].replace('Internal Theft & Fraud', 'Theft and Fraud', inplace=True)
df_input['Event_Type_L2'].replace('System Security External - Wilful Damage', 'Systems Security', inplace=True)
df_input['Event_Type_L2'].replace('System Security Internal - Wilful Damage', 'Systems Security', inplace=True)
df_input['Event_Type_L2'].replace('Unauthorized Activity', 'Unauthorised Activity', inplace=True)
df_input['Business_Line_L2'].replace('Municipal/Government Finance', 'Municipal / Government Finance', inplace=True)
df_input['Business_Line_L2'].replace('Treasury', 'Treasury / Funding', inplace=True)

# Clean Event Description
df_input['Event Description'] = df_input['Event Description'].apply(lambda text: clean_text(text))

# Clean Region column
df_input['Region'].replace("Asia / Pacific", "Asia", inplace=True)
df_input['Region'].replace("Other", "Asia", inplace=True)
df_input['Region'].replace("Western Europe", "Europe", inplace=True)
df_input['Region'].replace("Eastern Europe", "Europe", inplace=True)
df_input['Region'].replace("Other Americas", "Latin America & Caribbean", inplace=True)
df_input['Region'].replace("Other", "Asia", inplace=True)

################################################################################################################################################################
# Predict Fraud Labels
################################################################################################################################################################
print("- Predict Fraud Labels")
if df_input.shape[0]:
    # Convert to Dataset
    ds_input = Dataset.from_pandas(df_input)

    # Create Predictions
    ds_output = ds_input.map(
        lambda x: {"Probs": get_probs(x["NLP_Text"], tokenizer, model)}
    )
    ds_output = ds_output.map(
        lambda x: {"Labels": get_labels(x["Probs"], targets)}
    )

    # Convert back to DataFrame
    df_output = ds_output.to_pandas()

    # Split probs 
    df_output = pd.concat([df_output, pd.DataFrame(pd.DataFrame(df_output["Probs"].to_list(), columns=["Probs"])["Probs"].to_list(), columns = [f"prob_{label}" for label in targets])], axis=1).drop('Probs', axis=1)
                  
    # Concat old and new data
    df_output = pd.concat([df_output, df_previous_output], axis=0)

else:
    # No new rows: return output of previous refresh
    df_output = df_previous_output

# Clean Regions of Countries using all data
for country in list(df_output['Country'].unique()):
    df_country = df_output[df_output['Country'] == country] 
    df_output.loc[df_country.index, "Region"] = df_country['Region'].value_counts().index[0]
    del df_country

################################################################################################################################################################
# Export Excel
################################################################################################################################################################
print("- Export to excel")
df_output.to_excel(os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\Fraud Dashboard\Report Data\External Fraud Events.xlsx"), index=False)

print('# Preprocess Data Input End Time & Date: ' + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
print(100*'-')