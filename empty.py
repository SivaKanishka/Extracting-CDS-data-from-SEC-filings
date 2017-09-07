# -*- coding: utf-8 -*-
"""
Created on Mon Sep 04 15:28:23 2017

@author: Siva Kanihska
"""

# Loading neccessary libraries
import sqlite3
import requests
import pandas as pd
import re
import numpy as np
from bs4 import BeautifulSoup
# Getting urls of index files
import datetime
 
current_year = datetime.date.today().year
current_quarter = (datetime.date.today().month - 1) // 3 + 1
start_year = 1993
years = list(range(start_year, current_year))
quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
history = [(y, q) for y in years for q in quarters]
for i in range(1, current_quarter + 1):
    history.append((current_year, 'QTR%d' % i))
urls = ['https://www.sec.gov/Archives/edgar/full-index/%d/%s/master.idx' % (x[0], x[1]) for x in history]
urls.sort()

# Creating Database for indices
con = sqlite3.connect('edgar_idx.db')
cur = con.cursor()
cur.execute('DROP TABLE IF EXISTS idx')
cur.execute('CREATE TABLE idx (cik TEXT, conm TEXT, type TEXT, date TEXT, path TEXT)')

for url in urls[0:]:
    lines = requests.get(url).text.splitlines()
    records = [tuple(line.split('|')) for line in lines[11:]]
    cur.executemany('INSERT INTO idx VALUES (?, ?, ?, ?, ?)', records)
    print(url, 'downloaded and wrote to SQLite')

con.commit()
con.close()

# Connecting to the database
conn = sqlite3.connect('edgar_idx.db')
c = conn.cursor()
# Filtering the table for N-Q and N-CSR froms
c.execute('select * from idx where type = ? or type like ?', ('N-Q', '%CSR%'))
output1 = c.fetchall()

# Creating the input data frame
labels = ['CIK', 'Company_Name', 'Form_Type', 'Date', 'Filename']
input = pd.DataFrame.from_records(output1, columns=labels)
prefix = "https://www.sec.gov/Archives/"
input = input[['CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type']]
# Concatenation for url
input['URL'] = prefix + input[['Filename']]

# Creating unique ID
from sklearn.preprocessing import LabelEncoder

input["ID"] = LabelEncoder().fit_transform(input.CIK)
input = input[['ID', 'CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type', 'URL']]

# Downloading files
for i in range(0,10000):
    with open(input.at[i,'Filename'].replace('/', '_'), 'wb') as f:
        url = input.at[i, 'URL']
        f.write(requests.get(url).content)
        print(i, url, 'downloaded and wrote to text file')

# Initializing the data frame
dummy = pd.DataFrame(columns = ['ID', 'CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type', 'URL', 'Attributes', 'Values'])
# To capture the index of forms with CDS data, with table id
url_table_index = []

for u in range(0, 10000):
    ID = input['ID'][u]
    cik = input['CIK'][u]
    comp_name = input['Company_Name'][u]
    date = input['Date'][u]
    filename = input['Filename'][u]
    form_type = input['Form_Type'][u]
    url = input['URL'][u]
    with open(input.at[u,'Filename'].replace('/', '_'), 'r') as f:#
        soup = BeautifulSoup(f)
    all_tables = soup.find_all('table')
    x = []
    sum = 0
    for m in range(0, len(all_tables)):
        for row_value in all_tables[m].find_all('tr'):
            cds_columns = row_value.find_all('td')
            for cds_column in cds_columns:
                if cds_column.get_text().strip() not in ['Credit Default Swap', 'CDS', 'Credit Swap', 'Credit Default Swaps']:
                    break
                elif cds_column.get_text().strip() in ['Credit Default Swap', 'CDS', 'Credit Default Swaps', 'Credit Swap']:
                    url_table_index.append((u,m))
                    good_indices.append(u)
                    sum +=1
    if sum == 0: # Condition for appending NA values to non-CDS files
        x.append(np.nan)
        dummy = dummy.append(pd.DataFrame.from_items([('ID', ID), ('CIK', cik), ('Company_Name', comp_name), ('Date', date), ('Filename', filename), ('Form_Type', form_type), ('URL', url), ('Attributes', x), ('Values', x)]))
    print(u)


import pickle

with open("z_url_table_index.txt", "wb") as fp:   #Pickling
    pickle.dump(url_table_index, fp)

with open("z_url_table_index.txt", "rb") as fp:   # Unpickling
    url_table_index = pickle.load(fp)

