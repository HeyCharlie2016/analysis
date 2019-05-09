import os
import pdfkit

def convert_html_to_pdf(pdf_filename, html, PROJ_ROOT):
	# final_report_filepath = os.path.join(PROJ_ROOT,
	# 								"reports",
	# 								"final_reports")

	pdf_file_path = os.path.join(PROJ_ROOT,
										  "reports",
										  "final_reports",
										 # 'references',
										 # 'wkhtmltopdf',
 #                              "HeyCharlieReportTemplate.pdf")
										 pdf_filename)

	path_wkthmltopdf = os.path.join(PROJ_ROOT,
									'references',
									'wkhtmltopdf',
									'bin',
									'wkhtmltopdf.exe')

	config = pdfkit.configuration(wkhtmltopdf=path_wkthmltopdf)
	options = {
		'page-width': '11in',
		'page-height': '8.5in',
		#            'page-size': 'letter',
		'margin-top': '2cm',
		'margin-bottom': '2cm',
		'margin-left': '2.54cm',
		'margin-right': '2.54cm',
		'log-level': 'warn',
		#            'disable-smart-shrinking': True,
		#           'orientation': 'Landscape'
	}
	pdfkit.from_string(html, pdf_file_path, configuration=config, options=options)