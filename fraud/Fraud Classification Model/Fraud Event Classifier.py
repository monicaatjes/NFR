# -*- coding: utf-8 -*-
"""
Fine tune BERT model on labbeled external Fraud events data (ORX / SAS) 

Multiclass Classification model is created that predicts a single label out of more than 2 labels for each event in the data. 

Created on Fri Aug  4 09:13:30 2023

@author: UM98XA

Improvement IDEAS
    - Include other variables with prediction layer
    - Oversample minority classes
    
"""

from datetime import datetime
import pandas as pd
import os 
import numpy as np
import copy
import re
from sklearn.preprocessing import OneHotEncoder
import time
import matplotlib.pyplot as plt
import seaborn as sns

# BERT
import torch
from datasets import Dataset
from transformers import AutoTokenizer, DataCollatorWithPadding
from torch.utils.data import DataLoader
from transformers import AutoModelForSequenceClassification 
from transformers import AdamW
from transformers import get_scheduler
from tqdm.auto import tqdm

# Measure Runtime
start_time = time.time()

# Set seed
SEED = 1111
torch.manual_seed(SEED)

## Functions ######################################################################################################################################################################################################################################
# function: Clean NLP texts
def clean_text(text):
    # Clear Special UTF-8 Characters
    clean_text = " ".join(text.replace("_x000D_","").replace("\n", " ").replace("\xa0", " ").strip().split())
    
    # Clean constant texts
    clean_text = re.sub("Data supplied by ORX News.*\d{4}", "", clean_text)
    clean_text = re.sub("A DEEP DIVE IS NOW AVAILABLE FOR THIS LOSS EVENT ", "", clean_text) 
    return clean_text

# function: Tokenize NLP texts
def tokenize_func(batch):
    return tokenizer(batch["NLP_Text"], max_length=512, truncation=True, padding=True)

## Load Data ######################################################################################################################################################################################################################################
print('\n' + 100*"#")
print("# Load Data #")

# Set param
project_path = os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\Fraud Dashboard\Fraud Classification Model")
data_file = "Data\Fraud Event Labelling.xlsx"

keep_ORX = ['FRC Label Level 1 (Marco/Otto)', 'FRC Label Level 2 (Marco/Otto)', 'Story Reference Number', 'Headline', 'Digest Text', 
            'Scenario Category Name', 'Event Type Level 1 Name', 'Event Type Level 2 Name',
            'Business Line Level 1 Name', 'Business Line Level 2 Name',            
            'Region Name','Country Name',
            'Loss Amount (USD)']

keep_SAS = ['FRC Label Level 1 (Marco/Otto)', 'FRC Label Level 2 (Marco/Otto)', 'Reference ID Code', 'Description of Event',
            'Activity','Sub Risk Category','Event Risk Category',
            'Basel Business Line - Level 1', 'Basel Business Line - Level 2',
            'Region of Domicile','Country of Incident',
            'Loss Amount ($M)']

# Read data ORX and SAS
df_input_ORX = pd.read_excel(os.path.join(project_path, data_file), sheet_name= "ORX Check")
df_input_ORX = df_input_ORX[keep_ORX]
df_input_ORX['Source'] = "ORX"
df_input_SAS = pd.read_excel(os.path.join(project_path, data_file), sheet_name= "SAS Check")
df_input_SAS = df_input_SAS[keep_SAS]
df_input_SAS['Source'] = "SAS"

## Prep Data ######################################################################################################################################################################################################################################
# Create ID columns + remove old
df_input_ORX['ID'] = df_input_ORX['Source'] + "_" + df_input_ORX['Story Reference Number'].astype(str)
df_input_SAS['ID'] = df_input_SAS['Source'] + "_" + df_input_SAS['Reference ID Code'].astype(str)
del df_input_ORX['Story Reference Number'], df_input_SAS['Reference ID Code']

# Combine/Clean NLP Columns
nlp_col_ORX = ['Headline', 'Digest Text']
nlp_col_SAS = ['Description of Event']
for df, nlp_col in zip([df_input_ORX, df_input_SAS], [nlp_col_ORX, nlp_col_SAS]):
    # Combine
    df['NLP_Text'] = ''
    for column in nlp_col:
        df['NLP_Text'] = df['NLP_Text'] + ' ' + df[column]
        df.drop(column, axis=1, inplace=True)
    
    # Clean 
    df['NLP_Text'] = df['NLP_Text'].apply(lambda text: clean_text(text))
del df, nlp_col, column

# Clean Loss Columns
df_input_ORX['Loss Amount (USD)'] = pd.to_numeric(df_input_ORX['Loss Amount (USD)'], errors='coerce').fillna(0)
df_input_SAS['Loss Amount ($M)'] = df_input_SAS['Loss Amount ($M)']*(10**6)

# Rename columns to align datasets
df_input_ORX.rename(columns={'FRC Label Level 1 (Marco/Otto)': 'Label_L1', 'FRC Label Level 2 (Marco/Otto)': 'Label_L2',
                             'Scenario Category Name':'Scenario','Event Type Level 1 Name':'Event_Type_L1','Event Type Level 2 Name':'Event_Type_L2',
                             'Business Line Level 1 Name':'Business_Line_L1','Business Line Level 2 Name':'Business_Line_L2',
                             'Region Name':'Region','Country Name':'Country', 'Loss Amount (USD)':'Loss_USD'}, inplace=True)

df_input_SAS.rename(columns={'FRC Label Level 1 (Marco/Otto)': 'Label_L1', 'FRC Label Level 2 (Marco/Otto)': 'Label_L2',
                             'Activity':'Scenario','Event Risk Category':'Event_Type_L1','Sub Risk Category':'Event_Type_L2',
                             'Basel Business Line - Level 1':'Business_Line_L1','Basel Business Line - Level 2':'Business_Line_L2',
                             'Region of Domicile':'Region','Country of Incident':'Country', 'Loss Amount ($M)':'Loss_USD'}, inplace=True)

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

# Remove non-labbeled Rows and drop Label 2
df_input.dropna(subset=['Label_L1'], inplace=True)
del df_input['Label_L2']

# Split into X / Y and Train / Test
Y = copy.deepcopy(df_input)[['Label_L1']]
X = copy.deepcopy(df_input)[[col for col in df_input.columns if not col == 'Label_L1']]

# Encode (Y) labels
OHencoder = OneHotEncoder(handle_unknown='ignore', sparse = False).fit(Y)
Y_labels = [item.strip() for item in OHencoder.categories_[0]]
Y_enc = pd.DataFrame(OHencoder.transform(Y), index = Y.index, columns = Y_labels)
#Y_enc['labels'] = Y_enc[Y_labels].apply(lambda row: [float(v) for v in row], axis = 1)

# Drop 'No Fraud reporting Category' as label
Y_labels = [label for label in Y_labels if label != "No Fraud reporting category"]
Y_enc.drop("No Fraud reporting category", axis=1, inplace=True)
Y_enc['labels'] = Y_enc[Y_labels].apply(lambda row: [float(v) for v in row], axis = 1)

# Get Target Label dictionaries
Index2Label = {idx:label for idx, label in enumerate(Y_labels)}
Label2Index = {label:idx for idx, label in enumerate(Y_labels)}

# Encode/Prepare (X) features
X = X[['NLP_Text']] # For now only keep text column

# Recombine and convert to (HuggingFace) Dataset
df_train = pd.concat([Y_enc['labels'], X], axis=1)
ds_train = Dataset.from_pandas(df_train).train_test_split(test_size=0.2, seed=SEED)

## Setup BERT Model ######################################################################################################################################################################################################################################
# Set Checkpoint for BERT and get tokenizer + base model
checkpoint = "distilbert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(checkpoint, do_lower_case=True)
model = AutoModelForSequenceClassification.from_pretrained(checkpoint, num_labels=len(Y_labels),  #problem_type="multi_label_classification", 
                                                           id2label=Index2Label, label2id=Label2Index,
                                                           seq_classif_dropout=0.3, attention_dropout=0.1)

# Tokenize Texts + Setup Data collator / Loader
ds_train = ds_train.map(tokenize_func, batched=True)
ds_train_token = ds_train.remove_columns(["NLP_Text", 'ID'])
ds_train_token.set_format("torch")
data_collator = DataCollatorWithPadding(tokenizer=tokenizer)
data_loader = DataLoader(ds_train_token["train"], shuffle=True, batch_size=8, collate_fn=data_collator)

## Fine-tune BERT Model ######################################################################################################################################################################################################################################
print('\n' + 100*"#")
print("# Training Model #")

# Load Optimizer
optimizer = AdamW(model.parameters(), lr=5e-5, no_deprecation_warning=True)
num_epochs = 10
num_training_steps = num_epochs * len(data_loader)
lr_scheduler = get_scheduler(
    "linear",
    optimizer=optimizer,
    num_warmup_steps=0,
    num_training_steps=num_training_steps,
)

# Set loss function # No weigths
loss_fct = torch.nn.BCEWithLogitsLoss() # For Multi-Label / Or for Multi-CLass with unknown category
#loss_fct = torch.nn.CrossEntropyLoss() # For Multi-Class
 
# Set device to GPU (Doesn't work for INTEL GPU on windows yet)
device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
model.to(device)

# Set training Loop to fine-tune model
progress_bar = tqdm(range(num_training_steps))
model.train()
all_loss_hist = []
for epoch in range(num_epochs):
    print(f" First Batch (Epoch {epoch}): {datetime.now().strftime('%m/%d/%Y %H:%M:%S')}")
    loss_list = []
    for i, batch in enumerate(data_loader):
        if i + 1 == len(data_loader):
            # FOR Later: last 10 % for Data Validation
            print(f" Last Batch (Epoch {epoch}):  {datetime.now().strftime('%m/%d/%Y %H:%M:%S')}")
        
        # Send batch to GPU/CPU device
        batch = {k: v.to(device) for k, v in batch.items()}
        labels = batch.get("labels")    # Actuals
        
        # Forward Pass: Input data-batch into model and get outputs
        outputs = model(**batch)
        
        # Calculate Loss given outputs
        logits = outputs.get("logits")  # Predictions
        loss = loss_fct(logits, labels.type(torch.float64))
        loss_list.append(loss.item())
        
        # Backward pass: Update parameters and learning schedule/optimizer
        loss.backward() # Calculates the gradient for all parameters and updates them
        optimizer.step()
        lr_scheduler.step()
        optimizer.zero_grad() # Reset gradients
        progress_bar.update(1)
        
    # Print average loss of epoch
    print(" Epoch: {}, Loss: {}".format(epoch + 1, np.mean(loss_list)))
    all_loss_hist.append(np.mean(loss_list))

# Print Loss History
sns.set_style("darkgrid")
ax = sns.lineplot(data=pd.DataFrame(all_loss_hist, columns=["Loss"]), markers=True)
for x, y in enumerate(all_loss_hist): # label points on the plot
     # the position of the data label relative to the data point can be adjusted by adding/subtracting a value from the x &/ y coordinates
     plt.text(x = x, # x-coordinate position of data label
     y = y + 0.01, # y-coordinate position of data label, adjusted to be 150 below the data point
     s = "{0:.3g}".format(y) # data label, formatted to ignore decimals
     ) # set colour of line

# Save Results ######################################################################################################################################################################################################################################
# Save model
model.save_pretrained(os.path.join(project_path, "Classification Model", "Model - Fraud Classifier V2"))

# Save Dataset
try:
    ds_train.save_to_disk(os.path.join(project_path, "Classification Model", "Data - Train & Test"))
except:
    print("error saving dataset")
