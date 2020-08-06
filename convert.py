import os
from glob import glob
import pdfkit
from decimal import Decimal
from pathlib import Path

# You need to have the wkhtmltopdf.exe executable in this folder.
# Alternatively, add it to your PATH variables and comment out the config = line

fromdir = './htmls/'
outdir = './pdfs/'

Path(outdir).mkdir(parents=True, exist_ok=True)

config = pdfkit.configuration(wkhtmltopdf='./wkhtmltopdf.exe')

queue = [y for x in os.walk(fromdir) for y in glob(os.path.join(x[0], '*.html'))]

for i, f in enumerate(queue):
	f = f.replace('\\', '/')
	fDir = '/'.join(f.split('/')[:-1]).replace(fromdir, outdir) + '/'
	os.makedirs(fDir, exist_ok=True)
	try:
		pdfkit.from_file(
			f,
			f.replace(fromdir, outdir).replace('.html', '.pdf'),
			options = { 'enable-local-file-access': None },
			configuration = config
		)
	except Exception as e:
		print(e)
	print('Completed: {}%. Converted: {}'.format(int(i / len(queue) * 100), f))