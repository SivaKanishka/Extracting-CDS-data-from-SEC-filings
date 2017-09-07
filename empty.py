# -*- coding: utf-8 -*-
"""
Created on Mon Sep 04 15:28:23 2017

@author: Siva Kanihska
"""
import sqlite3
import pandas as pd
import re
import numpy as np
from bs4 import BeautifulSoup

conn = sqlite3.connect('edgar_idx.db')
c = conn.cursor()

c.execute('select * from idx where type = ? or type like ?', ('N-Q', '%CSR%'))
output1 = c.fetchall()


labels = ['CIK', 'Company_Name', 'Form_Type', 'Date', 'Filename']
input = pd.DataFrame.from_records(output1, columns=labels)
prefix = "https://www.sec.gov/Archives/"
input = input[['CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type']]
input['URL'] = prefix + input[['Filename']]

from sklearn.preprocessing import LabelEncoder

input["ID"] = LabelEncoder().fit_transform(input.CIK)
input = input[['ID', 'CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type', 'URL']]

dummy = pd.DataFrame(columns = ['ID', 'CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type', 'URL', 'Attributes', 'Values'])
good_indices = []
url_table_index = []

for u in range(0, 10000):
    ID = input['ID'][u]
    cik = input['CIK'][u]
    comp_name = input['Company_Name'][u]
    date = input['Date'][u]
    filename = input['Filename'][u]
    form_type = input['Form_Type'][u]
    url = input['URL'][u]
    with open(input.at[u,'Filename'].replace('/', '_'), 'r') as f:
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
    if sum == 0:
        x.append(np.nan)
        dummy = dummy.append(pd.DataFrame.from_items([('ID', ID), ('CIK', cik), ('Company_Name', comp_name), ('Date', date), ('Filename', filename), ('Form_Type', form_type), ('URL', url), ('Attributes', x), ('Values', x)]))
    print(u)


import pickle

with open("z_url_table_index.txt", "wb") as fp:   #Pickling
    pickle.dump(url_table_index, fp)

with open("z_final_indices.txt", "wb") as fp:   #Pickling
    pickle.dump(final_indices, fp)

with open("z_url_table_index.txt", "rb") as fp:   # Unpickling
    url_table_index = pickle.load(fp)

with open("z_final_indices.txt", "rb") as fp:   # Unpickling
    final_indices = pickle.load(fp)
