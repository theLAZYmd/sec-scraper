import os
import pdfkit
from pathlib import Path

# You need to add the wkhtmltopdf.exe executable to your path variables
# Run: setx PATH "%PATH%;C:\Program Files\wkhtmltopdf\bin" or wherever your executable is installed to add to path
# Alternatively, if you're running from this folder, setx PATH "%PATH%;./"

fromdir = './htmls/JA Solar/'
outdir = './pdfs/JA Solar/'

Path(outdir).mkdir(parents=True, exist_ok=True)

config = pdfkit.configuration(wkhtmltopdf='./wkhtmltopdf.exe')

for f in os.listdir(fromdir):
	if f.endswith('.html'):
		try:
			pdfkit.from_file(
				fromdir + f,
				outdir + f.replace('.html', '.pdf'),
				options = { 'enable-local-file-access': None },
				configuration = config
			)
		except Exception as e:
			print(e)
		print('Converted: ', f)