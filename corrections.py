# -*- coding: utf-8 -*-
"""
Created on Wed Sep 06 23:08:05 2017

@author: Siva Kanishka
"""
import pickle

with open("z_url_table_index.txt", "rb") as fp:   # Unpickling
    url_table_index = pickle.load(fp)

import pandas as pd
import re
import numpy as np
from bs4 import BeautifulSoup

df = pd.DataFrame(columns = ['ID', 'CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type', 'URL', 'Attributes', 'Values'])
# for capturing any missing values
missed = []
# to check if new rules should be created for scraping
missed_again = []
for (u,m) in url_table_index:
    ID = input['ID'][u]
    cik = input['CIK'][u]
    comp_name = input['Company_Name'][u]
    date = input['Date'][u]
    filename = input['Filename'][u]
    form_type = input['Form_Type'][u]
    url = input['URL'][u]
    #with open(input.at[i,'Filename'].replace('/', '_'), 'r') as f:
    #    soup = BeautifulSoup(f)
    with open(input.at[u,'Filename'].replace('/', '_'), 'r') as f:
        soup = BeautifulSoup(f)
    all_tables = soup.find_all('table')
    
    x = []
    y = []
    for row_value in all_tables[m].find_all('tr'):
        cds_columns = row_value.find_all('td')
        for cds_column in cds_columns:
            if cds_column.get_text().strip() in ['Credit Default Swap', 'CDS', 'Credit Swap']:# Checking for CDS Values in Columns
                attribute_index = all_tables[m].find_all('tr').index(row_value)-1
                attribute_columns = all_tables[m].find_all('tr')[attribute_index].find_all('td')
                attribute_names = ['CDS Text']
                for col in attribute_columns:
                    attribute_names.append(col.get_text().strip())
                attribute_names = list(filter(None, attribute_names))
                x = attribute_names
                if len(x) != 4:
                    x = ['CDS Text', 'Expiration Date', 'Notional Amount', 'Unrealized Appreciation/(Depreciation)']
                for k in range(1,21):
                    value_index = all_tables[m].find_all('tr').index(row_value)+k
                    if value_index <= len(all_tables[m].find_all('tr'))-1:
                        value_columns = all_tables[m].find_all('tr')[value_index].find_all('td')
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
                attribute_index = all_tables[m].find_all('tr').index(row_value)-2
                if attribute_index >= 0:
                    attribute_columns = all_tables[m].find_all('tr')[attribute_index].find_all('td')
                else:
                    attribute_index = attribute_index+1
                    attribute_columns = all_tables[m].find_all('tr')[attribute_index].find_all('td')
                attribute_names = []
                for col in attribute_columns:
                    attribute_names.append(col.get_text().strip())
                attribute_names = list(filter(None, attribute_names))
                x = attribute_names
                for s in range(1,21):
                    value_index = all_tables[m].find_all('tr').index(row_value)+s
                    if value_index <= len(all_tables[m].find_all('tr'))-1:
                        value_columns = all_tables[m].find_all('tr')[value_index].find_all('td')
                    else:
                        break
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
                        missed.append((u,m))
                        break
                break
    if len(x) == 0 and len(y) == 0:
        missed_again.append((u,m))
    print(u,m)