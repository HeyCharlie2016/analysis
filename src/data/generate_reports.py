import os
import datetime as dt
import pandas as pd

import make_dataset
import generate_report_data

import sys
# sys.path.append("../visualization/")
# TODO WTF - how to access files in other folders?
import generate_report_charts

# sys.path = [os.path.abspath(os.path.dirname(__file__))] + sys.path
import sys

# PROJ_ROOT = os.path.join(__file__,
# 							 os.pardir,
# 							 os.pardir,
# 							 os.pardir)
#
# visualization_path = os.path.join(PROJ_ROOT,
# 									"visualization")
# print(visualization_path)
# sys.path.append(visualization_path)
# print(sys.path)
# import visualization.generate_report_charts


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

	new_method = pd.date_range(today-dt.timedelta(28), today + dt.timedelta(0), freq='W-MON')
	# print('time indices comparison')
	# print(originale_method)
	print('Report Date Range')
	print(new_method)
	return new_method


if __name__ == '__main__':

	PROJ_ROOT = os.path.join(__file__,
							 os.pardir,
							 os.pardir,
							 os.pardir)

	PROJ_ROOT = os.path.abspath(PROJ_ROOT)

	usernames = ['liamkl', 'vinoct18', 'vinoct24', 'emily2', 'zombeck']
	date_indices = generate_date_indices()
	# print('max date')
	# print(max(date_indices).date())
	# TODO: Assert end data is not ahead of today for db checks
	usernames = make_dataset.refresh_user_data(usernames, PROJ_ROOT, max(date_indices).date())
	print(usernames)

	[comm_pie_chart_data, comm_days_line_chart_data, comm_vol_bar_chart_data, report_variables] = \
		generate_report_data.generate_report_data(usernames, date_indices, PROJ_ROOT)

	report_chart_path = os.path.join(PROJ_ROOT,
									"reports",
									"figures")
	generate_report_charts.comm_days_line_chart(usernames, date_indices, comm_days_line_chart_data, report_chart_path)
	generate_report_charts.comm_vol_bar_chart(usernames, date_indices[:-1], comm_vol_bar_chart_data, report_chart_path)
	# generate_html(usernames, report_variables)
