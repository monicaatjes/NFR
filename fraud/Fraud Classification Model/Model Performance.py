# -*- coding: utf-8 -*-
"""
Created on Mon Aug 28 10:53:48 2023

@author: UM98XA
"""

from datetime import datetime
import copy
import pandas as pd
import os 
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, roc_curve, roc_auc_score
import seaborn as sns
import openpyxl
sns.set(rc={'figure.figsize':(20,20)})

# BERT
import torch
from datasets import load_from_disk
from transformers import AutoTokenizer, DataCollatorWithPadding
from torch.utils.data import DataLoader
from transformers import AutoModelForSequenceClassification

## Load Data ######################################################################################################################################################################################################################################
project_path = os.path.join(r"C:\Users", os.environ['UserName'], r"ING\NFR Data & Analytics - General\Fraud Dashboard\Fraud Classification Model")
data_path = os.path.join(project_path, "Classification Model\Data - Train & Test V2")

# Load Dataset
ds_train = load_from_disk(data_path)

## Load BERT ######################################################################################################################################################################################################################################
# Model
model_path = os.path.join(project_path, "Classification Model\Model - Fraud Classifier V6")
model = AutoModelForSequenceClassification.from_pretrained(model_path)

# Load checkpoint
checkpoint = "distilbert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(checkpoint, do_lower_case=True)
data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

# set
Y_labels = ['Cybercrime',
             'Authorised  Push Payment Fraud',
             'Card Fraud',
             'Internal (Occupational) Fraud',
             'Lending Fraud',
             #'No Fraud reporting category',
             'Other Fraud',
             'Transaction Fraud',
             'e-Banking Fraud']

del data_path, model_path, checkpoint

## Compute Metrics ######################################################################################################################################################################################################################################
print('\n' + 100*"#")
print("# Compute Metrics & Store results #")

# Set device to GPU (Doesn't work for INTEL GPU on windows yet)
device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")

# Remove unnecessary columns and set to torch format
ds_copy = copy.deepcopy(ds_train) # Keep Copy for IDS + Texts
ds_train = ds_train.remove_columns(["NLP_Text", 'ID'])
ds_train.set_format("torch")

# Get Output & Metrics
def get_metrics(Y_act, Y_pred, f1_method):
    # Standard Metrics
    accuracy = (Y_pred == Y_act).mean()
    f1 = f1_score(Y_act, Y_pred, average=f1_method, zero_division=0)
    
    ## Own Metrics ##
    tot_pred = np.sum(np.sum(Y_pred))
    tot_act  = np.sum(np.sum(Y_act))
    Y_diff   = Y_act - Y_pred #Possible Matrix values: 0 predicted correct label (Y_act-i,j = Y_pred-i,j), 1 not-predicted label (Y_act-i,j = 1; Y_pred-i,j = 0), -1 predicted incorrect label (Y_act-i,j=0; Y_pred-i,j = 1)
    
    # Calculate %-metrics: % Correct Predictions, % Incorrect Predictions, % Unpredicted (missing) Labels
    tot_incorrect   = abs(np.sum(np.sum(Y_diff[Y_diff == -1])))
    tot_missing     = np.sum(np.sum(Y_diff[Y_diff == 1]))
    perc_incorrect  = tot_incorrect/tot_pred if tot_pred != 0 else np.nan
    perc_correct    = 1 - perc_incorrect 
    perc_missing    = tot_missing/tot_act if tot_act != 0 else np.nan
    return accuracy, f1, perc_correct, perc_incorrect, perc_missing

def evaluate_model(dataset, ID_arr):
    model.eval()
    
    # Define Arrays with IDs, targets & predictions
    df_IDs = pd.DataFrame(ID_arr, columns=["ID"])
    logits_arr = np.empty(shape=[0, len(Y_labels)])
    pred_arr = np.empty(shape=[0, len(Y_labels)])
    labels_arr = np.empty(shape=[0, len(Y_labels)])
    
    # Create dataloader
    data_loader = DataLoader(
        dataset, batch_size=8, collate_fn=data_collator, shuffle=False
    )
    
    # Loop to predict in batches
    for batch in data_loader:    
        batch = {k: v.to(device) for k, v in batch.items()}
        with torch.no_grad():
            outputs = model(**batch)
    
        # Get and store predictions
        logits = outputs.logits.sigmoid()
        logits_arr = np.concatenate([logits_arr, logits])
        #pred = (logits>0.5).float().numpy()  # With Threshold (Binary / Multi-Label)
        
        logits = logits*(logits>=0.20) # Subset probs that are larger than 0.20
        pred = (logits==logits.max(dim=1, keepdim=True)[0]).float().numpy() # With Max probability (Multi-Class)
        pred_arr = np.concatenate([pred_arr, pred])
        labels_arr = np.concatenate([labels_arr, batch['labels']])
        
    # Collect predictions and return
    metrics = get_metrics(labels_arr, pred_arr, 'micro') 
    df_results = pd.concat([df_IDs, pd.DataFrame(labels_arr, columns=Y_labels), pd.DataFrame(pred_arr, columns=['pred_' + targ for targ in Y_labels]), pd.DataFrame(logits_arr, columns=['prob_' + targ for targ in Y_labels])], axis = 1)
    return metrics, df_results

# Get metrics # 
metrics_train, df_results_train = evaluate_model(ds_train["train"], ds_copy["train"]["ID"])
metrics_test, df_results_test = evaluate_model(ds_train["test"], ds_copy["test"]["ID"])

# Print metrics
for ds_type, pred_metrics in zip(["Train", "Test"], [metrics_train, metrics_test]):
    print('\n' + 50*"-")
    print("# " + ds_type + " #")
    for i, metric in zip(range(0,len(pred_metrics)), ["Accuracy", "F1 score", "% Predicted Labels Correct", "% Predicted Labels Incorrect", "% Labels Not predicted"]):
        print("{} - {}: {}".format(ds_type, metric, pred_metrics[i]))
del ds_type, pred_metrics, i, metric

# Log metrics
def logmetrics(metrics_data):
    # Log Metrics #
    file_name = "Model Performance Scores.xlsx"
    file_path = os.path.join(project_path, file_name)
    
    # Save to workbook
    wb = openpyxl.load_workbook(file_path)
    ws = wb["Performance Metrics"]
    ws.append(list(metrics_data.values()))
    wb.save(file_path)
    wb.close()
    return

metrics_data = {'Run Date':datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), 
                  'Model':"DistilBERT",
                  'Accuracy (Train)':metrics_train[0],
                  'F1 Score (Train)':metrics_train[1],
                  '% Labels Correct (Train)':metrics_train[2],
                  '% Labels Incorrect (Train)':metrics_train[3],
                  '% Labels Missing (Train)':metrics_train[4],
                  'Accuracy (Test)':metrics_test[0],
                  'F1 Score (Test)':metrics_test[1],
                  '% Labels Correct (Test)':metrics_test[2],
                  '% Labels Incorrect (Test)':metrics_test[3],
                  '% Labels Missing (Test)':metrics_test[4],
                  'Best Param':"NA",
                  'Feature Selection Method':'NA',
                  'Feature Selection K Best':'NA',
                  'Comment':'V6 Revert back to Binary Cross Entropy as loss function + remove no fraud category as (explicit) target + subset probabilities lower than 0.2'}
logmetrics(metrics_data)

# Concat dataframes
df_results_train["Dataset"] = "train"
df_results_train["Text"] = ds_copy["train"]["NLP_Text"]
df_results_test["Dataset"] = "test"
df_results_test["Text"] = ds_copy["test"]["NLP_Text"]
df_results = pd.concat([df_results_test, df_results_train], axis = 0)

# Add other columns
df_results["Sum_Targets"] = df_results[Y_labels].apply(lambda row: sum(row),axis=1)
df_results["Sum_Preds"] = df_results[['pred_' + targ for targ in Y_labels]].apply(lambda row: sum(row),axis=1)
df_results['max_prob'] = df_results[['prob_' + targ for targ in Y_labels]].apply(lambda row: max(row),axis=1)

# Add Predicted Labels
df_results["Pred_Label"] = df_results[['pred_' + targ for targ in Y_labels]].apply(lambda row: ', '.join([label for label, ind in zip(Y_labels, row) if ind ==1]), axis = 1)
df_results["Pred_Label"].replace("", "No Fraud reporting category", inplace=True)

# Log Predictions in EXCEL
df_results.to_excel(os.path.join(project_path, "Classification Model", "Predictions Train-Test V6.xlsx"), index=False)
