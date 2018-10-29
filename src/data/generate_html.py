import os
import pandas as pd
import base64
from jinja2 import Environment, FileSystemLoader

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.image as mpimg


def get_chart_img(username, chart_name, report_chart_path):
	chart_file_path = os.path.join(report_chart_path, chart_name + '-' + username + '.png')
	data_uri = base64.b64encode(open(chart_file_path, 'rb').read()).decode('utf-8').replace('\n', '')
	chart_img = '<img src="data:image/png;base64,{0}">'.format(data_uri)
	return chart_img


def generate_html(usernames, report_variables, report_chart_path, PROJ_ROOT):
	resources_file_path = os.path.join(PROJ_ROOT,
									"references",
									"report_materials")
	comma_img_filepath = os.path.join(resources_file_path, 'Comma.png')
	html_template_filepath = 'HeyCharlieReportTemplateV2.html'
	final_report_filepath = os.path.join(PROJ_ROOT,
									"reports",
									"final_reports")

	# with open(html_template_filepath, "r", encoding='utf-8') as f:
	# 	html_text= f.read()
	# 	print(html_text[:200])

	data_uri = base64.b64encode(open(comma_img_filepath, 'rb').read()).decode('utf-8').replace('\n', '')
	comma_img = '<img src="data:image/png;base64,{0}">'.format(data_uri)
	env = Environment(loader=FileSystemLoader(resources_file_path, followlinks=True))

	# TODO move template to the resources folder
	# it on the base directory of the FileSystemLoader instead and then specify a template path
	# relative to that. with this? posixpath.join()
	template = env.get_template(html_template_filepath)

	for username in usernames:
		report_charts = ['weekly_comm_chart', 'comm_pie_chart', 'days_w_comm_chart']
		for chart in report_charts:
			chart_img = get_chart_img(username, chart, report_chart_path)
			report_variables[username][chart] = chart_img
		report_variables[username]['comma'] = comma_img

		# img = mpimg.imread(os.path.join(report_chart_path, 'CommPie-vinoct18.png'))
		# imgplot = plt.imshow(img)
		# plt.show()

		html_out = template.render(report_variables[username])
		html_filename = username + " HeyCharlieReport " + report_variables[username]['report_date'] + ".html"
		html_file_path = os.path.join(final_report_filepath, html_filename)
		html_file = open(html_file_path, "w")
		html_file.write(html_out)
		html_file.close()
	# print(report_variables['vinoct18']['CommPie'])
