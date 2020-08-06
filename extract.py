
import os
from glob import glob
import traceback
import pandas as pd
import lxml.etree
from bs4 import BeautifulSoup
import re
from write import xlsx
from decimal import Decimal
from pathlib import Path
import itertools

fromdir = './htmls/Canadian/'
outdir = './xlsx/'

Path(outdir).mkdir(parents=True, exist_ok=True)

queue = [y for x in os.walk(fromdir) for y in glob(os.path.join(x[0], '*.html'))]
regexes = {
    'year': re.compile(r"(?:2\s?0|1\s?9)\s?[0-9]\s?[0-9]$"),
    'currency': re.compile(r"^[$£]$")
}

class Extract:

    def __init__(self, table, name='', source='', preHeader=[''], columns=[''], length=0, postHeader = []):
        self.name = name + self.findName(table)
        self.source = source
        self.page = self.findPage(table)
        self.preHeader = preHeader
        self.length = length
        self.postHeader = postHeader
        self.currency = ''
        self.raw = []
        self.data = None
        
        dateRow = self.findDates(table)
        if dateRow == -1:
            return

        rows = table.find_all('tr')
        whilePost = True
        lineBreak = False
        for i, row in enumerate(rows):
            if i < dateRow:
                self.preHeader.append(row.text.strip())
                continue
            cells = row.select('th, td')
            values = [x.text.strip() for x in cells]
            if i == dateRow:
                self.columns = []
                for j, v in enumerate(values):
                    cell = cells[j]
                    self.columns.append(v)
                    if 'colspan' in cell:
                        for _ in itertools.repeat(None, int(cell['colspan'])):
                            self.columns.append('')
                self.length += len(self.columns)
                for j in range(0, len(self.columns) - self.length):
                    self.columns.append('')
                continue
            if not values[0] and whilePost:
                self.postHeader.append(row.text.strip())
                continue
            
            parsed = self.parseValues(values)
            if not len(values) or all([(not x) for x in values]):
                lineBreak = True
            elif lineBreak:
                self.raw.append([] * length)
                lineBreak = False

            self.raw.append(parsed)
            for _  in itertools.repeat(None, len(values) - len(self.columns)):
                self.columns.append('')

        if self.currency:
            self.name += ' ({})'.format(self.currency)
        df = pd.DataFrame(self.raw, columns=self.columns)
        self.data = df

    def parseValues(self, values):
        parsed = [None] * len(values)
        mware = [None] * len(values)
        for i, v in enumerate(values):
            if not v:
                parsed[i] = v
                continue
            if (v == ')' or v == '%' or v == ')%') and mware[i - 1]:
                mware[i - 1] += v
            elif regexes['currency'].match(v):
                pass
            else:
                if not mware[i]:
                    mware[i] = ''
                mware[i] += v
        for i, v in enumerate(mware):
            if not v:
                parsed[i] = v
                continue
            if v.startswith('(') and v.endswith(')'):
                v = '-' + v[1:-1]
            v = v.replace(',', '')
            if v == '—':
                parsed[i] = 0
                continue
            try:
                parsed[i] = float(v)
            except:
                parsed[i] = v
        return parsed

    def parseCurrency(self, values):
        for v in values:
            if regexes['currency'].match(v):
                return v
        return ''

    def findPage(self, table):     
        hr = table.find_next('hr')
        if hr:
            pageDiv = hr.find_previous('p')
            while not pageDiv.text:
                pageDiv = pageDiv.find_previous('p')
            if pageDiv.text.isnumeric():
                return int(pageDiv.text)
        x = len(table.find_all_previous('hr'))
        if x:
            return x
        return None

    def findName(self, table):
        b = table.find_previous('b')
        while b:
            t = b.text.strip()
            if not t:
                pass
            elif t.startswith('(') and t.startswith(')'):
                pass
            else:
                return t
            b = b.find_previous('b')
        self.name = table.find_previous('b').text.strip()


    ## Header
    def findDates(self, table):
        rows = table.find_all('tr')
        for i, row in enumerate(rows):
            cells = row.select('th, td')
            values = [x.text.strip() for x in cells if x.text.strip()]
            if len(values) and all([regexes['year'].search(v) and len(v) < 15 for v in values]):
                return i
        return -1


for i, f in enumerate(queue):
    f = f.replace('\\', '/')
    outDir = '/'.join(f.split('/')[:-1]).replace(fromdir, outdir) + '/'
    os.makedirs(outDir, exist_ok=True)
    print('\nExtracting tables from %s...' % (f))
    try:
        title = f.split('/')[-1].replace('.html', '')
        filename = title.split(' - ')
        [companyName, date, filing] = filename
        file = open(f)
        soup = BeautifulSoup(file, 'html.parser')
        tables = soup.find_all('table')
        compiled = {}
        for j, table in enumerate(tables):
            meta = Extract(table)
            if meta.data is None:
                continue
            meta.source = title + (', p. ' + str(meta.page) if meta.page else '')
            compiled[str(j)] = meta
            print('-- Converted: Table {} out of a possible {}'.format(j, len(tables)))   
        xlsx(compiled, outDir=outDir, companyName=companyName, workbookName=title) 
        print('Completed: {}%.'.format(int((i + 1) / len(queue) * 100)))
    except Exception as e:
        print(traceback.format_exc())
    if i:
        break