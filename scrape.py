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
import time
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
from write import xlsx


# ## 0: Set configuration variables
# 
# First section is customisable entries to choose data from the SEC
# Second section is output variables to design for the FTI format


# 0: Set configuration
companies = {
    'JA Solar': {
        'cik': '0001385598',
        'codes': ['6-K', '20-F'],
        # 'skip': True
    },
    'Canadian': {
        'cik': '0001375877',
        'codes': [
            '6-K',
            '20-F'
        ],
        # 'skip': True
    },
    'First': {
        'cik': '0001274494',
        'codes': ['10-Q', '10-K'],
        # 'skip': True
    },
    'Sun': {
        'cik': '0000867773',
        'codes': ['10-Q', '10-K'],
        # 'skip': True
    },
    'Trina': {
        'cik': '0001382158',
        'codes': ['6-K', '20-F'],
        # 'skip': True
    },
    'Hanwha Q': {
        'cik': '0001371541',
        'codes': ['6-K', '20-F'],
        # 'skip': True
    },
    'Jinko': {
        'cik': '0001481513',
        'codes': ['6-K', '20-F'],
        # 'skip': True
    }
}

workbookName = 'Disclosure Filings'         # ex: 'Disclosure Filings' which will be converted to Disclosure Filings.xlsx
datestart = '2015-01-01'
dateend = ''
base = 'https://www.sec.gov'

downloadHTMLs = True
outFolder = './data/'
outHTMLFolder = './htmls/'

# Set configuration for output file (xlsx and downloads)
pageBreakSize = 3
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
if len(sys.argv) > 1:
    if sys.argv[1] == '--test':
        isTest = True
if isTest:
    maxrows = 2

class Scrape:

    def __init__(self, companyName='', cik='', codes=[], datestart = '', dateend = ''):
        
        print('\nStarting download process for %s...' % (companyName))
        self.companyName = companyName
        self.cik = cik
        self.codes = codes
        self.datestart = datestart
        self.dateend = dateend
        self.workbookTitle = ''
        self.url = ''
        self.debug_url = ''

        self.folder = outFolder + companyName + '/'
        self.fileFolder = outHTMLFolder + companyName + '/'
        if isTest:
            Path(self.folder).mkdir(parents=True, exist_ok=True)
        Path(self.fileFolder).mkdir(parents=True, exist_ok=True)

        self.output = {}

    async def run(self):
        arr = []
        for k in self.codes:
            xml = self.get(k)
            obj = self.convert(xml)
            a = await self.parse(obj, companyName=self.companyName)
            for filing in a:
                for k in filing.keys():
                    if k.startswith('_'):
                        try:
                            del a[k]
                        except:
                            pass
            arr.extend(a)
            
        if isTest:
            # Creates an intermediate XML output. Mainly for debugging purposes. Comment out if undesirable.
            f = open(self.folder + self.companyName + '.xml', 'w')
            f.write(xml)
            f.close()

            # Creates an intermediate JSON output. Mainly for debugging purposes. Comment out if undesirable.
            f = open(self.folder + self.companyName + '.json', 'w')
            y = json.dumps(arr, indent=4, default=str)
            f.write(y)
            f.close()

            # Creates an intermediate CSV output. Alternative to pandas.
            f = open(self.folder + self.companyName + '.csv', 'w', newline='')
            fieldnames = self.getColumns(arr, '')
            r = csv.DictWriter(
                f,
                delimiter=',',
                quotechar='"',
                fieldnames=fieldnames,
                extrasaction='ignore'
            )
            r.writeheader()
            r.writerows(arr)
            f.close()

        # Creates main .xlsx output using pandas.
        obj = {}
        for code in self.codes:
            kept = list(filter(lambda x: x['type'] == code, arr))
            if not len(kept):
                continue
            columns = self.getColumns(kept, code)
            df = pd.DataFrame(kept, columns=columns)
            obj[self.companyName + ' ' + code] = df
        return obj

    # ### 1: Get the data from the SEC website

    def get(self, t=''):
        params = {
            'action': 'getcompany',
            'start': self.datestart or datestart or 0,
            'type': t,
            'dateb': self.dateend or dateend,
            'owner': '',
            'search_text': '',
            'CIK': self.cik,
            'count': 100,
            'output': 'atom'
        }
        r = requests.get(base + '/cgi-bin/browse-edgar', params)
        print('-- Indexing: ', r.url)
        self.url = r.url
        return r.text


    # ### 2. Convert the resulting XML into a python dict

    def convert(self, data):
        return xmltodict.parse(data)


    # ### 3. Handle the dict to remove unwanted terms and select only the data needed

    async def parse(self, data, companyName=''):
        table = []
        rows = []
        filingPromises = []
        downloadPromises = []
        
        if not self.workbookTitle:
            self.workbookTitle = data['feed']['company-info']['conformed-name']
        
        if 'entry' in data['feed']:
            for i, e in enumerate(data['feed']['entry']):
                if not self.filterScraped(e):
                    continue
                if maxrows != None and i >= maxrows:
                    break

                # Date handling
                d = datetime.datetime.fromisoformat(e['content']['filing-date'])
                date = self.getDate(d)
                row = {
                    'date': date,
                    'type': e['category']['@term'],
                    'index': e['link']['@href'],
                    'source': '',
                    '_datetime': d
                }
                filingPromises.append(self.links(row['index']))
                rows.append(row)
        filings = await asyncio.gather(*filingPromises)

        for i in range(0, len(rows)):
            filing = { **rows[i], 'source': locate({ **rows[i], **filings[i]}) }
            rows[i] = filing
            downloadPromises.append(self.downloadFile({ **rows[i], **filings[i]}))
        downloads = await asyncio.gather(*downloadPromises)

        writingPromises = []

        for i in range(0, len(rows)):
            filing = rows[i]
            if downloads[i]:
                filing['title'] = self.getTitle(downloads[i], filing)
                filing['pages'] = self.getPages(downloads[i])
                if not self.isStatement(filing):
                    continue
                filing['quarter'] = self.getQuarter(filing)
                filing['reference'] = self.getReference(filing)
                if downloadHTMLs:
                    writingPromises.append(self.writeFile(downloads[i], rows[i]))
            else:
                filing['pages'] = 0
                if filing['type'] == '6-K':
                    continue
            table.append(filing)
            
        await asyncio.gather(*writingPromises)
        
        return table

    def filterScraped(self, e):
        if e['category']['@term'] not in self.codes:
            return False
        if datestart and e['content']['filing-date'] < (self.datestart or datestart):
            return False
        if dateend and e['content']['filing-date'] > (self.dateend or dateend):
            return False
        return True

    async def writeFile(self, html, filing):
        name = '{}{} - {} - {}.html'.format(self.fileFolder, self.companyName, filing['reference'], filing['type'])
        f = open(name, 'w', encoding='utf-8')
        f.write(html)
        f.close()

    # ### 4. Link pulling and cacheing

    async def links(self, url):
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

    async def downloadFile(self, filing):
        file = locate(filing)
        if not file:
            print('-- Err: no filing listed: %s' % [x for x in filing.keys()])
            return
        try:
            print('-- Fetching: ' + file)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
            }
            r = requests.get(file, headers=headers)
        except:
            print('-- Err: malformed URL: ', file)
            return
        self.debug_url = r.url
        html = r.text
        if html.startswith('<?xml'):
            html = '<html><body' + html.split('<body')[1]
        return html

    # ### 5. Metadata parsing

    def getDate(self, v):
        temp = datetime.datetime(1899, 12, 30)
        delta = v - temp
        return float(delta.days) + (float(delta.seconds) / 86400)

    def getPages(self, html):
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

    def getTitle(self, html, filing):
        if filing['type'] == '6-K':
            x = self.getTitleByP(html)
            if not x:
                x = self.getTitleByDiv(html)
            return x
        else:
            return self.getTitleByDiv(html)


    def getTitleByP(self, html):
        try:
            d = pq(html)
        except:
            title = ''
            matches = re.findall(r"<p.*><b>.*[a-zA-Z]+.*<\/b><\/p>", html)
            for m in matches:
                groups = re.search(r"<p.*><b>.*([a-zA-Z]+).*<\/b><\/p>", m)
                if not len(groups.groups()):
                    continue
                group = groups.group(1)
                bolds = re.findall(r"<b>(?:<.*?>)?([\w\s]+)(?:<\/.*?>)?<\/b>", group)
                for b in bolds:
                    gs = re.search(r"<b>(?:<.*?>)?([\w\s]+)(?:<\/.*?>)?<\/b>", b)
                    if not len(gs.groups()):
                        continue
                    bold = gs.group(1)
                    if 'exhibit' in bold.lower():
                        continue
                    if bold == '&nbsp;':
                        continue
                    title += bold
                    break
            return title
        arr = d.items('p')
        elem = None
        for i, a in enumerate(arr):
            if i > 10:
                break
            if not len(a.find('b')):
                continue
            if not a.text().strip():
                continue
            if any(s in a.text().strip().lower() for s in ['exhibit', 'united states', 'securities and exchange commission', 'washington, d.c. 20549']):
                continue
            elem = a
            break
        if not elem:
            return ''
        text = elem.text().replace('\n', ' ')
        return text

    def getTitleByDiv(self, html):
        try:
            d = pq(html)
        except:
            title = ''
            matches = re.findall(r"<div.*><span>.*[a-zA-Z]+.*<\/span><\/div>", html)
            for m in matches:
                groups = re.search(r"<div.*><span>.*([a-zA-Z]+).*<\/span><\/div>", m)
                if not len(groups.groups()):
                    continue
                group = groups.group(1)
                bolds = re.findall(r"<span.*?style=.*?font-weight:\s?bold.*?>(?:<.*?>)??(?:[\w\s-]|&nbsp;)+(?:<\/.*?>)??<\/span>", group)
                for b in bolds:
                    gs = re.search(r"<span.*?style=.*?font-weight:\s?bold.*?>(?:<.*?>)??((?:[\w\s-]|&nbsp;)+)(?:<\/.*?>)??<\/span>", b)
                    if not len(gs.groups()):
                        continue
                    bold = gs.group(1)
                    b = bold.lower().strip()
                    if not b:
                        continue
                    if 'exhibit' in b:
                        continue
                    if bold == '&nbsp;':
                        continue
                    s = re.search(r"For\s+the\s+(fiscal\s+year|quarterly\s+period)\s+ended:?\s+(\w+\s+\d{1,2},\s+\d{1,4})", b)
                    if not s:
                        continue
                    title += s.group(0)
                    break
            return title
        arr = [x for x in d.items('div, p, td')]
        elem = None
        i = 0
        for a in arr:
            if not a.text().strip():
                continue
            if i > 50:
                break
            i += 1
            if not a.find(self.generateSelectors(
                elements=['span', 'font'], attribs=['style'], keys=['font-weight'], values=['bold', '700']
            ) + ', b') and 'font: bold' not in a.attr('style'):
                continue
            if not re.search(r"For\s+the\s+(fiscal\s+year|quarterly\s+period)\s+ended:?\s+(\w+\s+\d{1,2},\s+\d{1,4})", a.text().strip()):
                continue
            elem = a
            break
        if not elem:
            return ''
        s = re.search(r"For\s+the\s+(fiscal\s+year|quarterly\s+period)\s+ended:?\s+(\w+\s+\d{1,2},\s+\d{1,4})", elem.text().replace('\n', ' '))
        if not s:
            return ''
        return s.group(0)

    def generateSelectors(self, elements=[], attribs=[], keys=[], values=[]):
        selectors = []
        for e in elements:
            for a in attribs:
                for k in keys:
                    for v in values:
                        selectors.append('{}[{}*="{}:{}"]'.format(e, a, k, v))
                        selectors.append('{}[{}*="{}: {}"]'.format(e, a, k, v))
        return ', '.join(selectors)

    def getQuarter(self, filing):
        if filing['type'] != '6-K' and filing['type'] != '10-Q':
            return None
        t = filing['title'].lower()
        if 'first quarter' in t or 'march' in t or 'april' in t:
            return 1
        if 'second quarter' in t or 'june' in t or 'july' in t:
            return 2
        if 'third quarter' in t or 'september' in t or 'october' in t:  # september 30 or october 1
            return 3
        if 'fourth quarter' in t or 'december' in t or 'january' in t:
            return 4
        return None

    def getReference(self, filing):
        matches = re.search(r"[0-9]\s?[0-9]\s?[0-9]\s?[0-9]", filing['title']) 
        if not matches:
            return ''
        year = matches[0].replace(' ', '')
        if filing['quarter']:
            matches = re.search(r"[0-9]\s?[0-9]\s?[0-9]\s?[0-9]", filing['title'])
            if not matches:
                return ''
            return year + '-Q' + str(filing['quarter'])
        else:
            return int(year)

    def isStatement(self, filing):
        if filing['type'] != '6-K':
            return True
        if filing['pages'] < 2:
            return False
        t = filing['title'].lower()
        if 'host' in t or 'conference' in t:
            return False
        if 'quarter' in t and 'results' in t:
            return True
        return False

    # ### 6. Get fieldnames

    def getColumns(self, arr, code):
        # Note that set().union(*(d.keys() for d in arr)) does the same but doesn't preserve order, which we want
        f = ['date', 'quarter', 'reference', 'type', 'title', 'pages', 'index']
        for obj in arr:
            for k in obj.keys():
                if k not in f:
                    if not k.startswith('_'):
                        f.append(k)
        f.remove('quarter')
        return f


    # ### 7. Run and write
    # 
    # The main output function called below in main()
    # Writes to an .xlsx file in the same directory with variable name specified above.
    # Formats it in the FTI style automatically. Variables abstracted out above for more personal control.
    # This function's a bit long and messy because of the nature of xlsxwriter maintains the need to keep things in global scope. Read the comments.

    # ### Run

async def main():
    output = {}
    for companyName, data in companies.items():
        if 'skip' in data and data['skip']:
            continue
        s = Scrape(companyName=companyName, cik=data['cik'], codes=data['codes'])
        obj = await s.run()
        for k in obj.keys():
            obj[k].name = s.workbookTitle
            obj[k].src = '{} SEC Filings ({}-{}), {}/cgi-bin/browse-edgar?CIK={}'.format(s.workbookTitle, datestart.split('-')[0], dateend.split('-')[0], base, data['cik'])
        output.update(obj)
    xlsx(output, workbookName=workbookName)

asyncio.run(main())

# Aloysius Lip 2020
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
