#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import pandas as pd


# In[2]:


# Read content from the file
with open(r"C:\Users\10485\Desktop\Big Data Analytics\project 358\Data\StudentGrades.txt") as file:
    content = file.read()


# In[3]:


# Extract data for each section
pattern = r'GRADES - (.*?)\n[-]+.*?\n([\s\S]*?)(?=\n\n|$)'
matches = re.findall(pattern, content)

data = []


for examname, section_content in matches:
    # Extract student ID and grade in each section
    grades_pattern = r'(S\d+)\s*–\s*(\d+)'
    grades_matches = re.findall(grades_pattern, section_content)
    
    # Append to the data list
    for sid, result in grades_matches:
        data.append([sid, examname, int(result)])


# In[4]:


#rescale final result as out of 100
df = pd.DataFrame(data, columns=["sid", "examname", "result"])
df['examname'] = df['examname'].str.replace('FINAL (OUT OF 200)', 'Final')
df.loc[df['examname'] == 'Final', 'result'] = (df.loc[df['examname'] == 'Final', 'result'] / 200) * 100
print(df)


# In[8]:


df.reset_index(inplace=True)
df.rename(columns={'index': 'old_index'}, inplace=True)
df['rowkey'] = df.index + 1
df = df[['rowkey'] + [col for col in df.columns if col != 'rowkey']]
del df['old_index']
print(df)


# In[9]:


# Export DataFrame to CSV file
df.to_csv(r"C:\Users\10485\Desktop\StudentGrades_updated.csv", index=False)

