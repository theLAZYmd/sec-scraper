import os
import xmltodict
import openpyxl
import xlsxwriter
import numpy as np
import pandas as pd
import pandas.io.formats.excel


def xlsx(tables={}, companyName='', header='', source='', start_row=7, start_column=1, colour='#44556a', insertLogo='./FTI.jpg', workbookName='', outDir='', date_format='yyyy-mm-dd', other_headers=False):
	# pylint: disable=abstract-class-instantiated
    f = pd.ExcelWriter(outDir.replace('/', '\\') + workbookName + '.xlsx', engine='xlsxwriter')
	
    def capitalize(s):
        s = str(s)
        # Format empty strings as Other here too, why not
        if not s:
            if other_headers:
                return 'Other'
            else:
                return ''
        return s[0].capitalize() + s[1:]

    for sheet_name, data in tables.items():
        df = None
        if isinstance(data, pd.DataFrame):
            df = data
            if 'name' in df:
                companyName = df.name
            if 'src' in df:
                source = df.src
        else:
            df = data.data
            source = data.source
            header = data.name
        
        df.to_excel(
            f,
            sheet_name=sheet_name,
            startcol=start_column,
            startrow=start_row,
            index=False
        )
        workbook = f.book
        worksheet = f.sheets[sheet_name]

        std = { 'border': 0, 'bg_color': 'white', 'num_format': '#,##0;[Black](#,##0)' }

        # Default formats for any FTI sheet
        std_fmt = workbook.add_format(std)
        dt_format = workbook.add_format({ **std, 'num_format': date_format, 'align': 'left' })
        bold_fmt = workbook.add_format({ **std, 'bold': True })
        highlighted_fmt = workbook.add_format({ **std, 'bold': True, 'color': 'white', 'bg_color': colour })
        header_format = workbook.add_format({ **std, 'bold': True, 'align': 'left' })
        header_format.set_bottom(1)
        header_format.set_top(1)
        footer_format = workbook.add_format({ **std, 'italic': True })
        footer_format.set_top(2)

        # Header formats
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(start_row, start_column + col_num, capitalize(value), header_format)
        
        # Write headers
        worksheet.write('B2', companyName, bold_fmt)
        worksheet.write('B3', workbookName, bold_fmt)
        worksheet.write('B4', 'FTI Consulting', bold_fmt)
        if insertLogo:
            worksheet.insert_image('D2', insertLogo)

        # Header content
        worksheet.write(start_row - 2, start_column, header or sheet_name, highlighted_fmt)
        for col_num in range(start_column + 1, start_column + len(df.columns.values)):
            worksheet.write(start_row - 2, col_num, '', highlighted_fmt)
        
        # Footer content
        worksheet.write(start_row + len(df) + 1, start_column, 'Source: {}'.format(source) , footer_format)
        for col_num in range(start_column + 1, start_column + len(df.columns.values)):
            worksheet.write(start_row + len(df) + 1, col_num, '', footer_format)

        # Write columns        
        worksheet.set_column(0, start_column - 1, 2, std_fmt)
        for i, header in enumerate(df.columns.values):
            if 'date' in header.lower():
                worksheet.set_column('B:B', 12, dt_format)
            elif 'title' in header.lower() and len(df[header]):
                worksheet.set_column(start_column + i, start_column + i, round(df[header].map(len).mean()), std_fmt) # TODO: filter out no content from average
            else:
                worksheet.set_column(start_column + i, start_column + i, None, std_fmt)
        worksheet.set_column(start_column + len(df.columns.values), start_column + len(df.columns.values) + 10, None, std_fmt)
            
        # Write rows
        #for i in range(0, max(start_row + 1 + len(df) + 10, 30)):
        #    worksheet.set_row(i, None, std_fmt)

    workbook.close()
    print('Wrote to: {}\\{}{}.xlsx'.format(os.getcwd(), outDir.replace('/', '\\'), workbookName))
