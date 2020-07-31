#!/usr/bin/env python
# coding: utf-8

# This tool:
# 1. Downloads all the listings within a given time range from an SEC listing of a company
# 2. Saves all the pages as HTML files
# 3. Compiles an Excel spreadsheet, formatted to FTI specifications, summarising the listing.  
# 
# Use a tool found in the same folder, named `convert.py` to convert the HTML results into PDF files using the wkhtmltopdf.exe utility.

# In[500]:


# Comment out the following lines if not running this program in Jupyter and use pip install the normal way.
 '''
get_ipython().system(' pip install requests')
get_ipython().system(' pip install xmltodict')
get_ipython().system(' pip install datetime')
get_ipython().system(' pip install asyncio')
get_ipython().system(' pip install pyquery')
get_ipython().system(' pip install pathlib')
get_ipython().system(' pip install openpyxl')
get_ipython().system(' pip install numpy')
get_ipython().system(' pip install pandas')
 '''

# In[501]:


import requests
import xmltodict
import asyncio
import datetime
from pyquery import PyQuery as pq
from pathlib import Path
import sys
import os
import re

import openpyxl
import xlsxwriter
import numpy as np
import pandas as pd
import pandas.io.formats.excel
import csv
import json


# ## 0: Set configuration variables
# 
# First section is customisable entries to choose data from the SEC
# Second section is output variables to design for the FTI format

# In[587]:


# 0: Set configuration

companyName = 'JA Solar'           # Short name, only for file naming purposes
workbookTitle = ''              # This is populated automatically but you can choose to specify if you so wish
cik = '0001385598'              # We can improve this code to fetch cik from name if so needed
keepCodes = ['6-K', '20-F']    # Specify which filings to keep. Write custom functions for data to extract in function locate()
                                # ex: ['10-K', '1O-Q', '6-K', '20-F']
datestart = '2015-01-01'        # If going very far back in time this script needs to be modified since it only gets the 1st 100 entries
dateend = ''
base = 'https://www.sec.gov'

downloadHTMLs = True
outFolder = './data/'
outHTMLFolder = './htmls/'

# Set configuration for output file (xlsx and downloads)
start_row = 7
start_column = 1
fti_colour = '#44556a'
pageBreakSize = 3
insertLogo = './FTI.jpg'         # Leave as empty string to not insert logo
maxrows = None
isTest = False                   # Test runs fetch less data

# Download selection configuration
def locate(filing):
    f = ''
    if filing['type'] == '6-K':
        f = filing.get('EX-99.1', '')
    elif filing['type'] == '20-F':
        f = filing.get('20-F', '')
    elif filing['type'] == '10-K':
        f = filing.get('10-K', '')
    elif filing['type'] == '10-Q':
        f = filing.get('10-Q', '')
    else:
        f = filing.get(filing['type'], '')        
    if 'ix?doc=/' in f:
        f = f.replace('ix?doc=/', '')
    return f

# Don't set this
folder = outFolder + companyName + '/'
fileFolder = outHTMLFolder + companyName + '/'
if isTest:
    maxrows = 2
elif len(sys.argv) > 1:
    if sys.argv[1] == '--test':
        maxrows = 2


# ### 1: Get the data from the SEC website

# In[588]:


def get(type=''):
    params = {
        'action': 'getcompany',
        'start': 0,
        'type': type,
        'dateb': dateend,
        'owner': '',
        'search_text': '',
        'CIK': cik,
        'count': 100,
        'output': 'atom'
    }
    r = requests.get(base + '/cgi-bin/browse-edgar', params)
    print('Index page: ', r.url)
    return r.text


# ### 2. Convert the resulting XML into a python dict

# In[589]:


def convert(data):
    return xmltodict.parse(data)


# ### 3. Handle the dict to remove unwanted terms and select only the data needed

# In[590]:


async def parse(data):
    table = []
    rows = []
    filingPromises = []
    downloadPromises = []
    
    global workbookTitle
    workbookTitle = data['feed']['company-info']['conformed-name']
    
    if 'entry' in data['feed']:
        for i, e in enumerate(data['feed']['entry']):
            if not filterScraped(e):
                continue
            if maxrows != None and i > maxrows:
                break
            row = {
                'date': e['content']['filing-date'],
                'type': e['category']['@term'],
                'index': e['link']['@href']
            }
            filingPromises.append(links(row['index']))
            rows.append(row)
    filings = await asyncio.gather(*filingPromises)

    for i in range(0, len(rows)):
        filing = { **rows[i],  **filings[i] }
        rows[i] = filing
        downloadPromises.append(downloadFile(filing))
    downloads = await asyncio.gather(*downloadPromises)

    writingPromises = []

    for i in range(0, len(rows)):
        filing = rows[i]
        if downloads[i]:
            if not isStatement(downloads[i], filing):
                continue
            filing['pages'] = getPages(downloads[i])
            if downloadHTMLs:
                writingPromises.append(writeFile(downloads[i], rows[i]))
        else:
            filing['pages'] = 0
            if filing['type'] == '6-K':
                continue
        table.append(filing)
        
    await asyncio.gather(*writingPromises)
    
    return table

def filterScraped(e):
    if e['category']['@term'] not in keepCodes:
        return False
    if datestart and e['content']['filing-date'] < datestart:
        return False
    if dateend and e['content']['filing-date'] > dateend:
        return False
    return True


# ### 4. Link pulling and cacheing

# In[591]:


async def links(url):
    d = pq(url=url)
    parent = d('table.tableFile tr')
    obj = {}
    for row in parent:
        p = d(row)
        link = p('td:nth-child(3) > a')
        href = link.attr('href')
        if not href:
            continue
        if href.startswith('/'):
            href = base + href
        obj[p('td:nth-child(4)').text()] = href
    return obj

async def downloadFile(filing):
    file = locate(filing)
    if not file:
        print('Err: no filing listed: ', filing)
        return
    try:
        print('Fetching: ' + file)
        r = requests.get(file)
    except:
        print('Err: malformed URL: ', file)
        return
    html = r.text
    return html

def getPages(html):
    try:
        d = pq(html)
    except:
        n = html.count('page-break-before') + html.count('page-break-after')
        if not n:
            n = html.count('<hr')
        return n
    
    breaks = d('[style*="page-break-before:always"], [style*="page-break-after:always"], [style*="page-break-before: always"], [style*="page-break-after: always"]')
    if not len(breaks):
        breaks = d('hr[size="' + str(pageBreakSize) + '"], hr[noshade]')
    return len(breaks)

def isStatement(html, filing):
    if filing['type'] != '6-K':
        return True
    try:
        d = pq(html)
    except:
        text = html.lower()
        if re.search(r"<b>.*reports.*quarter.*</b>", text):
            return True
        return False
    bolds = [i.text().lower() for i in d.items('b')]
    canReturn = [False, False]
    for i, text in enumerate(bolds):
        if i > 7:
            break
        if 'quarter' in text:
            canReturn[0] = True
        if 'results' in text:
            canReturn[1] = True
        if canReturn[0] and canReturn[1]:
            return True
    return False

async def writeFile(html, filing):    
    name = ' - '.join([companyName, filing['date'], filing['type']])
    f = open(fileFolder + '' + name + '.html', 'w', encoding='utf-8')
    f.write(html)
    f.close()


# ### 5. Get fieldnames

# In[592]:


def getFields(arr):
    # Note that set().union(*(d.keys() for d in arr)) does the same but doesn't preserve order, which we want
    f = ['date', 'type', 'pages', 'index']
    for obj in arr:
        for k in obj.keys():
            if k not in f:
                f.append(k)
    return f

def capitalize(str):
    # Format empty strings as Other here too, why not
    if not str:
        return 'Other'
    return str[0].capitalize() + str[1:]


# ### 6. Run and write
# 
# The main output function called below in main()
# Writes to an .xlsx file in the same directory with variable name specified above.
# Formats it in the FTI style automatically. Variables abstracted out above for more personal control.
# This function's a bit long and messy because of the nature of xlsxwriter maintains the need to keep things in global scope. Read the comments.

# In[593]:


def write(arr):
    f = pd.ExcelWriter(companyName + ' - SEC' + '.xlsx', engine='xlsxwriter')

    for code in keepCodes:
        kept = list(filter(lambda x: x['type'] == code, arr))
        columns = getFields(kept)
        df = pd.DataFrame(kept, columns=columns)
        df.to_excel(
            f,
            sheet_name=code,
            startcol=start_column,
            startrow=start_row,
            index=False
        )
        workbook = f.book
        worksheet = f.sheets[code]

        # Header formats
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': 'white',
            'border': 0,
            'align': 'left'
        })
        header_format.set_bottom(1)
        header_format.set_top(1)
        for col_num, value in enumerate(df.columns.values):
               worksheet.write(start_row, col_num + 1, capitalize(value), header_format)

        # Footer formats
        footer_format = workbook.add_format({
            'bg_color': 'white',
            'border': 0,
        })
        footer_format.set_top(2)
        for col_num in range(0, len(df.columns.values)):
            worksheet.write(start_row + 1 + len(df), col_num + 1, '', footer_format)

        # Default formats for any FTI sheet
        std_fmt = workbook.add_format({ 'border': 0, 'bg_color': 'white' })
        bold_fmt = workbook.add_format({ 'bold': True, 'border': 0, 'bg_color': 'white' })
        highlighted_fmt = workbook.add_format({ 'bold': True, 'border': 0, 'color': 'white', 'bg_color': fti_colour })

        worksheet.set_column('C:Z', None, std_fmt)
        worksheet.set_column(0, start_column - 1, 2, std_fmt)
        worksheet.set_column('B:B', 12, std_fmt)
        worksheet.write('B2', companyName + ' - ' + cik, bold_fmt)
        worksheet.write('B3', workbookTitle, bold_fmt)
        worksheet.write('B4', 'FTI Consulting', bold_fmt)
        
        if insertLogo:
            worksheet.insert_image('D2', insertLogo)

        starty = datestart.split('-')[0]
        endy = dateend.split('-')[0] or str(datetime.date.today().year)
        worksheet.write(start_row - 2, 1, code + ' (' + starty + '-' + endy + ')', highlighted_fmt)
        for col_num in range(1, len(df.columns.values)):
            worksheet.write(start_row - 2, col_num + 1, '', highlighted_fmt)
            
    workbook.close()
    print('Wrote to: ', os.getcwd() + '\\' + companyName + ' - SEC.xlsx')


# ### Run

# In[594]:


async def main():
    print('Starting download process...')
    Path(folder).mkdir(parents=True, exist_ok=True)
    Path(fileFolder).mkdir(parents=True, exist_ok=True)

    arr = []
    for k in keepCodes:
        xml = get(k)
        obj = convert(xml)
        a = await parse(obj)
        arr.extend(a)
        
    fieldnames = getFields(arr)
    y = json.dumps(arr, indent=4)

    # Creates an intermediate XML output. Mainly for debugging purposes. Comment out if undesirable.
    f = open(folder + companyName + '.xml', 'w')
    f.write(xml)
    f.close()

    # Creates an intermediate JSON output. Mainly for debugging purposes. Comment out if undesirable.
    f = open(folder + companyName + '.json', 'w')
    f.write(y)
    f.close()

    # Creates an intermediate CSV output. Alternative to pandas.
    f = open(folder + companyName + '.csv', 'w', newline='')
    r = csv.DictWriter(
        f,
        delimiter=',',
        quotechar='"',
        fieldnames=fieldnames
    )
    r.writeheader()
    r.writerows(arr)
    f.close()

    # Creates main .xlsx output using pandas.
    write(arr)

await main()


# Aloysius Lip 2020
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
