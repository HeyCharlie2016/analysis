import os
import base64
from jinja2 import Environment, FileSystemLoader


def get_chart_img(username, chart_name, report_chart_path):
	chart_file_path = os.path.join(report_chart_path, chart_name + '-' + username + '.png')
	data_uri = base64.b64encode(open(chart_file_path, 'rb').read()).decode('utf-8').replace('\n', '')
	chart_img = '<img src="data:image/png;base64,{0}">'.format(data_uri)
	return chart_img


def generate_html(usernames, report_variables, report_chart_path, PROJ_ROOT):
	resources_file_path = os.path.join(PROJ_ROOT,
									"references",
									"report_materials")
	final_report_filepath = os.path.join(PROJ_ROOT,
									"reports",
									"final_reports")

	comma_img_filepath = os.path.join(resources_file_path, 'Comma.png')
	data_uri = base64.b64encode(open(comma_img_filepath, 'rb').read()).decode('utf-8').replace('\n', '')
	comma_img = '<img src="data:image/png;base64,{0}">'.format(data_uri)

	env = Environment(loader=FileSystemLoader(resources_file_path, followlinks=True))
	template = env.get_template('HeyCharlieReportTemplateV2.html')

	for username in usernames:
		report_charts = ['weekly_comm_chart', 'comm_pie_chart', 'days_w_comm_chart', 'days_w_risky_loc_chart']
		for chart in report_charts:
			chart_img = get_chart_img(username, chart, report_chart_path)
			report_variables[username][chart] = chart_img
		report_variables[username]['comma'] = comma_img

		html_out = template.render(report_variables[username])
		html_filename = username + " HeyCharlieReport " + report_variables[username]['report_date'] + ".html"
		html_file_path = os.path.join(final_report_filepath, html_filename)
		html_file = open(html_file_path, "w")
		html_file.write(html_out)
		html_file.close()
