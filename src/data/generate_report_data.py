import os
import pandas as pd
import datetime as dt
import numpy as np
import json


def comm_pie_chart(comm_df, comm_pie_chart_data, username, date):
	# cols = ['risky_percent', 'neutral_percent', 'supportive_percent', 'unrated_percent']
	# change to a multi-index to match the other data tables?
	cols = comm_pie_chart_data.columns
	# user_xs = comm_pie_chart_data.xs(username)
	if date in comm_df.index:
		# comm_pie_chart_data.loc[(username, date), cols] = comm_df.loc[date, cols]
		data = comm_df.loc[date, cols]
		# replace this loop with a merge
		for e in cols:
			comm_pie_chart_data.loc[username, e] = data[e]
		# data.index = [username]
		# # comm_pie_chart_data = comm_pie_chart_data.append(data, ignore_index=True)
		return comm_pie_chart_data.fillna(0)
	print("no pie chart data")
	return comm_pie_chart_data.fillna(0)


def multi_index_chart_data(source_data_df, chart_data_df, username):
	cols = chart_data_df.columns

	# TODO: source_data_df gets a [nan] entry randomly inserted when accessed by this function
	# the following line helps but it is still a listr
	source_data_df = source_data_df.fillna(0)

	user_xs = chart_data_df.xs(username)
	date_indices = user_xs.index
	for date in date_indices:
		if date in source_data_df.index:
			chart_data_df.loc[(username, date), cols] = source_data_df.loc[date, cols]

	# chart_data_df.replace('[nan]', 0) # doesn't seem to be working - because it's not NaN, it's [nan]
	return chart_data_df.fillna(0)


def generate_report_variables(username, report_variables, comm_df, location_log_df, date_indices, locations_df):
	report_week = date_indices[-2].date()
	today = dt.date.today()

	avg_weekly_total_comm = comm_df['total_comm'][min(date_indices).date():report_week].mean()
	avg_weekly_risky_comm = comm_df['risky_comm'][min(date_indices).date():report_week].mean()

	if report_week in comm_df.index:
		day_with_max_total_comm = comm_df.loc[report_week, 'high_total_comm_day']
		day_with_max_risky_comm = comm_df.loc[report_week, 'high_risky_comm_day']
		change_in_risky_comm = comm_df['change_in_risky_comm'][report_week]
		change_in_risky_comm_days = comm_df['change_in_risky_comm_days'][report_week]
	else:
		day_with_max_total_comm = np.nan
		day_with_max_risky_comm = np.nan
		change_in_risky_comm = np.nan
		change_in_risky_comm_days = np.nan

	if report_week in location_log_df.index:
		day_with_max_risky_loc = location_log_df['high_risky_loc_visits_day'][report_week]
		change_in_risky_comm = '{:.0%}'.format(change_in_risky_comm)
		change_in_risky_loc_days = location_log_df['change_in_days_w_risky_loc_visits'][report_week]
	else:
		day_with_max_risky_loc = np.nan
		change_in_risky_loc_days = np.nan

	known_risky_loc = len(locations_df.loc[locations_df['type'] == 'risky'])
		# TODO: not all of these should be NA based on comm date

	report_variables[username] = {"title": "HeyCharlie User Report: " + username + " " + today.strftime('%b %d %Y'),
								"user_name": username,
								"clinic_ID": 'Genesis Counseling Services',
								"report_date": today.strftime('%b %d %Y'),
								"report_week": date_indices[-2].strftime('%b %d'),
								"change_in_risky_comm": change_in_risky_comm,
								"change_in_risky_comm_days": '{:+.0f}'.format(change_in_risky_comm_days) + " Days",
								"change_in_risky_loc_days": '{:+.0f}'.format(change_in_risky_loc_days) + " Days",
								"avg_weekly_total_comm": '{:.0f}'.format(avg_weekly_total_comm),
								"avg_weekly_risky_comm": '{:.0f}'.format(avg_weekly_risky_comm),
								"day_with_max_total_comm": day_with_max_total_comm,
								"day_with_max_risky_comm": day_with_max_risky_comm,
								"day_with_max_risky_loc": day_with_max_risky_loc,
								"known_risky_loc": '{:.0f}'.format(known_risky_loc)}
	return report_variables


def generate_report_data(usernames, date_indices, PROJ_ROOT):
	# Get DataSets:
	interim_data_path = os.path.join(PROJ_ROOT,
									"data",
									"interim")
	report_data_path = os.path.join(PROJ_ROOT,
									"reports",
									"report_variables")

	# Initialize the chart data dataframes
	multi_index = pd.MultiIndex.from_product([usernames, date_indices], names=['usernames', 'date'])
	comm_pie_chart_cols = ['risky_percent', 'neutral_percent', 'supportive_percent', 'unrated_percent']
	comm_pie_chart_data = pd.DataFrame(np.nan, index=usernames, columns=comm_pie_chart_cols)
	comm_days_line_chart_cols = ['total_comm_days', 'risky_comm_days', 'supportive_comm_days']
	comm_days_line_chart_data = pd.DataFrame(np.nan, index=multi_index, columns=comm_days_line_chart_cols)
	comm_vol_bar_chart_cols = ['total_comm', 'risky_comm', 'neutral_comm', 'supportive_comm', 'unrated_comm']
	comm_vol_bar_chart_data = pd.DataFrame(np.nan, index=multi_index, columns=comm_vol_bar_chart_cols)
	loc_days_bar_chart_cols = ['days_w_risky_loc_visits']
	loc_days_bar_chart_data = pd.DataFrame(np.nan, index=multi_index, columns=loc_days_bar_chart_cols)
	report_variables = {}

	report_date = max(date_indices).date()
	print("report_date")
	print(report_date)

	# Cycle through each user, populating the chart data dataframes
	for username in usernames:
		interim_comm_data_file_path = os.path.join(interim_data_path, 'week_comm_log_df_' + username + '.pkl')
		weekly_comm_df = pd.read_pickle(interim_comm_data_file_path)
		interim_loc_data_file_path = os.path.join(interim_data_path, 'week_loc_log_df_' + username + '.pkl')
		weekly_loc_log_df = pd.read_pickle(interim_loc_data_file_path)
		locations_data_file_path = os.path.join(interim_data_path, 'locations_df_' + username + '.pkl')
		locations_df = pd.read_pickle(locations_data_file_path)

		comm_pie_chart_data = comm_pie_chart(weekly_comm_df, comm_pie_chart_data, username, report_date - dt.timedelta(7))
		comm_days_line_chart_data = multi_index_chart_data(weekly_comm_df, comm_days_line_chart_data, username)
		comm_vol_bar_chart_data = multi_index_chart_data(weekly_comm_df, comm_vol_bar_chart_data, username)
		loc_days_bar_chart_data = multi_index_chart_data(weekly_loc_log_df, loc_days_bar_chart_data, username)
		report_variables = generate_report_variables(username, report_variables, weekly_comm_df, weekly_loc_log_df,
													 date_indices, locations_df)

	comm_pie_chart_data.to_pickle(os.path.join(report_data_path, 'comm_pie_chart_data.pkl'))
	comm_days_line_chart_data.to_pickle(os.path.join(report_data_path, 'comm_days_line_chart_data.pkl'))
	comm_vol_bar_chart_data.to_pickle(os.path.join(report_data_path, 'comm_vol_bar_chart_data.pkl'))
	loc_days_bar_chart_data.to_pickle(os.path.join(report_data_path, 'loc_days_bar_chart_data.pkl'))
	with open(os.path.join(report_data_path, 'report_variables.txt'), 'w') as file:
		file.write(json.dumps(report_variables))

	# print(loc_days_bar_chart_data.index)
	# print(loc_days_bar_chart_data)
	# print(report_variables)
	return comm_pie_chart_data, comm_days_line_chart_data, comm_vol_bar_chart_data, loc_days_bar_chart_data, report_variables
