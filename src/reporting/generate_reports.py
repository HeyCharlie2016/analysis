import os
import datetime as dt
import pandas as pd
import csv

import sys
# add the 'src' directory as one where we can import modules
PROJ_ROOT = os.path.join(__file__,
						 os.pardir,
						 os.pardir,
						 os.pardir)
src_dir = os.path.join(PROJ_ROOT, "src")
sys.path.append(src_dir)
import generate_html
from visualization import generate_report_charts
from data import make_dataset
from data import generate_report_data


def generate_date_indices():
	# TBD manual entry date ranges
	today = dt.date.today()
	# if today.weekday() == 0:
	# 	last_monday = today - dt.timedelta(7)
	# else:
	# 	last_monday = today - dt.timedelta(today.weekday())
	#
	# start = last_monday - dt.timedelta(21)
	# end = last_monday + dt.timedelta(7)
	# original_method = pd.date_range(start, end, freq='7D')

	new_method = pd.date_range(today - dt.timedelta(35), today + dt.timedelta(0), freq='W-MON')
	# print('time indices comparison')
	# print(originale_method)
	print('Report Date Range')
	print(new_method)
	return new_method


def parse_usernames(datafile):
	data = []
	n = 0
	with open(datafile, "r") as f:
		data_reader = csv.reader(f)
		for i, row in enumerate(data_reader):
			data.append(row)
	print(data[0])
	return data[0]


if __name__ == '__main__':
	PROJ_ROOT = os.path.join(__file__,
							 os.pardir,
							 os.pardir,
							 os.pardir)

	PROJ_ROOT = os.path.abspath(PROJ_ROOT)

	report_usernames_path = os.path.join(PROJ_ROOT,
										 "reports",
										 'report_usernames.csv')

	usernames = parse_usernames(report_usernames_path)

	print(usernames)
	# usernames = ['liamkl', 'vinoct18', 'vinoct24', 'emily2', 'zombeck']
	date_indices = generate_date_indices()
	print(date_indices)
	# print('max date')
	# print(max(date_indices).date())
	# TODO: Assert end data is not ahead of today for db checks
	usernames = make_dataset.refresh_user_data(usernames, PROJ_ROOT, max(date_indices).date())

	[comm_pie_chart_data, comm_days_line_chart_data, comm_vol_bar_chart_data, loc_days_bar_chart_data,
		report_variables] = generate_report_data.generate_report_data(usernames, date_indices, PROJ_ROOT)

	report_chart_path = os.path.join(PROJ_ROOT,
									"reports",
									"figures")
	generate_report_charts.comm_days_line_chart(usernames, date_indices[:-1], comm_days_line_chart_data, report_chart_path)
	generate_report_charts.comm_vol_bar_chart(usernames, date_indices[:-1], comm_vol_bar_chart_data, report_chart_path)
	generate_report_charts.comm_pie_chart(usernames, date_indices[-2], comm_pie_chart_data, report_chart_path)
	generate_report_charts.loc_days_bar_chart(usernames, date_indices[:-1], loc_days_bar_chart_data, report_chart_path)

	generate_html.generate_html(usernames, report_variables, report_chart_path, PROJ_ROOT)
