# -*- coding: utf-8 -*-
"""
Created on Wed Sep 06 19:06:23 2017

@author: Chebolu
"""
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

import sqlite3
import requests

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

import pandas as pd
import re
import numpy as np

data.to_csv('index_data.csv', sep = '\t')

conn = sqlite3.connect('edgar_idx.db')
c = conn.cursor()

c.execute('select * from idx where type = ? or type like ?', ('N-Q', '%CSR%'))
output1 = c.fetchall()

conn.close()

labels = ['CIK', 'Company_Name', 'Form_Type', 'Date', 'Filename']
input = pd.DataFrame.from_records(output1, columns=labels)
prefix = "https://www.sec.gov/Archives/"
input = input[['CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type']]
input['URL'] = prefix + input[['Filename']]

from sklearn.preprocessing import LabelEncoder

input["ID"] = LabelEncoder().fit_transform(input.CIK)
input = input[['ID', 'CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type', 'URL']]

input.to_csv('input.csv')
output1 = pd.DataFrame.from_records(output1)
output1.to_csv('output1.csv')

import urllib2
from bs4 import BeautifulSoup
df = pd.DataFrame(columns = ['ID', 'CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type', 'URL', 'Attributes', 'Values'])
missed = []
for i in range(0,10000):
    ID = input['ID'][i]
    cik = input['CIK'][i]
    comp_name = input['Company_Name'][i]
    date = input['Date'][i]
    filename = input['Filename'][i]
    form_type = input['Form_Type'][i]
    url = input['URL'][i]
    #with open(input.at[i,'Filename'].replace('/', '_'), 'r') as f:
    #    soup = BeautifulSoup(f)
    page = urllib2.urlopen(url)
    soup = BeautifulSoup(page)
    all_tables = soup.find_all('table')
    
    x = []
    y = []
    for j in range(0, len(all_tables)):
        for row_value in all_tables[j].find_all('tr'):
            cds_columns = row_value.find_all('td')
            for cds_column in cds_columns:
                if cds_column.get_text().strip() in ['Credit Default Swap', 'CDS', 'Credit Swap']:
                    attribute_index = all_tables[j].find_all('tr').index(row_value)-1
                    attribute_columns = all_tables[j].find_all('tr')[attribute_index].find_all('td')
                    attribute_names = ['CDS Text']
                    for col in attribute_columns:
                        attribute_names.append(col.get_text().strip())
                    attribute_names = list(filter(None, attribute_names))
                    x = attribute_names
                    if len(x) != 4:
                        x = ['CDS Text', 'Expiration Date', 'Notional Amount', 'Unrealized Appreciation/(Depreciation)']
                    for k in range(1,21):
                        value_index = all_tables[j].find_all('tr').index(row_value)+k
                        if value_index <= len(all_tables[j].find_all('tr'))-1:
                            value_columns = all_tables[j].find_all('tr')[value_index].find_all('td')
                        else:
                            break
                        values = []
                        for col_val in value_columns:
                            values.append(col_val.get_text().strip())
                        values = list(filter(None, values))
                        y = values
                        if len(y) == 4:
                            df = df.append(pd.DataFrame.from_items([('ID', ID), ('CIK', cik), ('Company_Name', comp_name), ('Date', date), ('Filename', filename), ('Form_Type', form_type), ('URL', url), ('Attributes', x), ('Values', y)]))
                            k = k+1
                        else:
                            break
                    break
                elif cds_column.get_text().strip() in ['Credit Default Swaps']:
                    attribute_index = all_tables[j].find_all('tr').index(row_value)-2
                    attribute_columns = all_tables[j].find_all('tr')[attribute_index].find_all('td')
                    attribute_names = []
                    for col in attribute_columns:
                        attribute_names.append(col.get_text().strip())
                    attribute_names = list(filter(None, attribute_names))
                    x = attribute_names
                    for s in range(1,21):
                        value_index = all_tables[j].find_all('tr').index(row_value)+s
                        value_columns = all_tables[j].find_all('tr')[value_index].find_all('td')
                        values = []
                        for col_val in value_columns:
                            values.append(col_val.get_text().strip())
                        values = list(filter(None, values))
                        if len(values) != 4 and "$" in values:
                            values.remove("$")
                        elif ")" in values:
                            values.remove(")")
                        y = values
                        if len(y) == 4:
                            df = df.append(pd.DataFrame.from_items([('ID', ID), ('CIK', cik), ('Company_Name', comp_name), ('Date', date), ('Filename', filename), ('Form_Type', form_type), ('URL', url), ('Attributes', x), ('Values', y)]))
                            s = s+1
                        else:
                            missed.append((i,j))
                            break
                    break
    if len(x) == 0 and len(y) == 0:
        x.append(np.nan)
        y.append(np.nan)
        df = df.append(pd.DataFrame.from_items([('ID', ID), ('CIK', cik), ('Company_Name', comp_name), ('Date', date), ('Filename', filename), ('Form_Type', form_type), ('URL', url), ('Attributes', x), ('Values', y)]))
    elif len(x) != 0 and len(y) == 0:
        x = np.nan
        y.append(np.nan)
        df = df.append(pd.DataFrame.from_items([('ID', ID), ('CIK', cik), ('Company_Name', comp_name), ('Date', date), ('Filename', filename), ('Form_Type', form_type), ('URL', url), ('Attributes', x), ('Values', y)]))
    print(i)

writer = pd.ExcelWriter('output1_1000.xlsx', engine='xlsxwriter')
df.to_excel(writer, sheet_name='Sheet1')
writer.save()


