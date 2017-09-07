# -*- coding: utf-8 -*-
"""
Created on Thu Sep 07 01:26:14 2017

@author: Siva Kanishka
"""
import pandas as pd
import numpy as np
import re

# Loading data ad data frame
final = pd.read_excel(open('output1_10000.xlsx','rb'), sheetname='Sheet1')
# Filtering rows wtrh NA values
qf = final.dropna()

uniq_val = list(set(qf['Attributes']))
# Renaming the Attribute name of CDS text
qf['Attributes'] = qf.Attributes.replace('Description', 'CDS Text')
# Filtering rows to get data frame with CDS text
qf = qf[qf.Attributes == 'CDS Text']

qf = qf[['ID', 'CIK', 'Company_Name', 'Date', 'Filename', 'Form_Type', 'URL', 'Values']]

qf = qf.rename(columns = {'Values':'CDS_Text'})

cds = list(qf['CDS_Text'])

# Regeex to capture the company or firm names
z = re.compile(r"default(.+)? of ([A-Z][A-Za-z]+,? of ([A-Z][A-Za-z]+.?,? ?)+|[A-Z][A-Za-z]+,? ([A-Z][A-Za-z]+.?,? ?)+)")
count = re.compile(r"([A-Z][A-Za-z]+,? of ([A-Z][A-Za-z]+.?,? ?)+|[A-Z][A-Za-z]+,? ([A-Z][A-Za-z]+.?,? ?)+)")

# Underlying identification
under = []
for i in range(0,len(cds)):
    text = cds[i]
    text = text.strip().replace('\n', ' ')
    (start1, end1) = z.search(text).span()
    text = text[start1:end1]
    (start2, end2) = count.search(text).span()
    under.append(text[start2:end2].strip().strip(','))
    print i

# Actual underlying
reg_actual = re.compile(r"(\d+[^A-Za-z]+% [a-z]+ [^A-Za-z]+/\d\d|\d+[^A-Za-z]+% [^A-Za-z]+/\d\d)")
actual_underlying = []
for j in range(0,len(cds)):
    text = cds[j]
    text = text.strip().replace('\n', ' ')
    actual = reg_actual.search(text)
    if actual != None:
        actual_underlying.append(under[j] + ' ' + text[actual.start():actual.end()])
    else:
        actual_underlying.append(under[j] + ' ' + 'default of issues')
    print(j)

# Counterparty identification
counterparty = []
for k in range(0,len(cds)):
    counter = []
    t = []
    text = cds[k]
    text = text.strip().replace('\n', ' ')
    for match in re.finditer(r"([A-Z][A-Za-z]+,? of ([A-Z][A-Za-z]+.?,? ?)+|[A-Z][A-Za-z]+ ([A-Z][A-Za-z]+.?,? ?)+|pay [A-Z][A-Za-z]+,? upon)", text):
        t = match.group()
        t = t[:-1].strip().strip(',')
        counter.append(t)
    index = [counter.index(c) for c in counter if c != under[k]]
    counterparty.append(counter[index[0]])
    print(k)

# CDS_Transaction identification
buy = re.compile(r"((P|p)ay quarterly|(P|p)ay.+year(ly)?)")
sell = re.compile(r"((R|r)eceive quarterly|(R|r)eceive (quarterly|annually|year(ly)?))")
trans = []
miss = []
for l in range(0,len(cds)):
    text = cds[l]
    text = text.strip().replace('\n', ' ')
    if buy.search(text) != None:
        trans.append('BUY')
    elif sell.search(text) != None:
        trans.append('SELL')
    else:
       miss.append(l) 
    print(l)

# Appending columns to data frame
qf['Underlying'] = under
qf['Exact Underlying'] = actual_underlying
qf['CounterParty'] = counterparty
qf['CDS Transaction'] = trans

# Writing data frame to an Excel workbook
writer = pd.ExcelWriter('output2_10000.xlsx', engine='xlsxwriter')
qf.to_excel(writer, sheet_name='Sheet1')
writer.save()
