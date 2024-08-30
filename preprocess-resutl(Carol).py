#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
from datetime import datetime


# In[2]:


# Get the target folder path
folder_path = r"C:\Users\10485\Desktop\Big Data Analytics\project 358\Data\Data"

# Create an empty list to store DataFrames
dfs = []
dfs1 = []


# In[3]:


for root, dirs, files in os.walk(folder_path):
    for file in files:
        # Check if the file is a TEMP.csv, BVP.csv, EDA.csv, or HR.csv file
        if file.endswith(".csv") and file.split('.')[0] in ["BVP","EDA","HR","TEMP"]:
            file_path = os.path.join(root, file)
            
            # Read the file content
            df = pd.read_csv(file_path, header=None)
            
            # Extract file name and folder names
            file_name = os.path.basename(file_path)
            type_name = os.path.splitext(file_name)[0]
            folder_name = os.path.dirname(file_path)
            folders = folder_name.split("\\")
            folder_col1, folder_col2 = folders[-2], folders[-1]
            
            # Process data
            timestamp = int(df.iloc[0, 0])
            sample_rate = int(df.iloc[1, 0])
            data_values = df.iloc[2:].astype(float)
            time_range = pd.date_range(start=pd.to_datetime(timestamp, unit='s'), periods=len(data_values), freq=f'{1/sample_rate}S')
            df = pd.DataFrame({'Timestamp': time_range, 'Data': data_values[0].tolist()})
            
            # Add "type", "sid", and "examname" columns to the DataFrame
            df['type'] = type_name
            df['sid'] = folder_col1
            df['examname'] = folder_col2
            
            # Append the DataFrame to the list
            dfs.append(df)  


# In[4]:


# Concatenate all DataFrames in the list
result_df = pd.concat(dfs, ignore_index=True)

# Reset index to renumber the data
result_df.reset_index(drop=True, inplace=True)

# Print the result DataFrame
print(result_df)


# In[5]:


dfs1 = []
for root, dirs, files in os.walk(folder_path):
    for file in files:
        # Check if the file is a IBI.csv file
        if file.endswith(".csv") and file.split('.')[0] in ["IBI"]:
            file_path = os.path.join(root, file)
            
            # Read the file content
            df1 = pd.read_csv(file_path, header=None)
            
            # Extract file name and folder names
            file_name = os.path.basename(file_path)
            type_name = os.path.splitext(file_name)[0]
            folder_name = os.path.dirname(file_path)
            folders = folder_name.split("\\")
            folder_col1, folder_col2 = folders[-2], folders[-1]
            
            # Process data
            start_timestamp = df1.iloc[0, 0]
            data_list = []
            for index, row in df1.iloc[1:].iterrows():
                interval = row[0]
                data = row[1]
                timestamp = start_timestamp + interval
                readable_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                data_list.append([readable_time, data])
                
            df = pd.DataFrame(data_list, columns=['Timestamp', 'Data'])
            
            # Add "type", "sid", and "examname" columns to the DataFrame
            df['type'] = type_name
            df['sid'] = folder_col1
            df['examname'] = folder_col2
            
            # Append the DataFrame to the list
            dfs1.append(df)  


# In[6]:


# Concatenate all DataFrames in the list
result_df1 = pd.concat(dfs1, ignore_index=True)

# Reset index to renumber the data
result_df1.reset_index(drop=True, inplace=True)

# Print the result DataFrame
print(result_df1)


# In[7]:


result = pd.concat([result_df, result_df1], ignore_index=True)
print(result)


# In[8]:


result.reset_index(inplace=True)
result.rename(columns={'index': 'old_index'}, inplace=True)
result['rowkey'] = result.index + 1
result = result[['rowkey'] + [col for col in result.columns if col != 'rowkey']]
del result['old_index']
print(result)


# In[9]:


# Export DataFrame to CSV file
result.to_csv(r"C:\Users\10485\Desktop\result.csv", index=False)


# In[ ]:




