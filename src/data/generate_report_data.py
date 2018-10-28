import os
import pandas as pd
import datetime as dt
import numpy as np


# Future steps: input report templates and only generate the needed data
#
# Get DataSets:
# 	Check that the datasets exist,
# 	else call for them

# pull chart data:
# 	 Apply date ranges to data sets
# 	Each chart gets it's own file including all the users
#   return a dictionary with all the chart data

def comm_pie_chart(comm_df, comm_pie_chart_data, username, date):
	# cols = ['risky_percent', 'neutral_percent', 'supportive_percent', 'unrated_percent']
	#change to a multi-index to match the other data tables?
	cols = comm_pie_chart_data.columns
	if date in comm_df.index:
		data = comm_df[cols].loc[date]
		# replace this loop with a merge
		for e in cols:
			comm_pie_chart_data.loc[username, e] = data[e]
		# data.index = [username]
		# # comm_pie_chart_data = comm_pie_chart_data.append(data, ignore_index=True)
		return comm_pie_chart_data
	print("no pie chart data")
	return comm_pie_chart_data


def comm_days_line_chart(comm_df, comm_days_line_chart_data, username):
	cols = comm_days_line_chart_data.columns
	user_xs = comm_days_line_chart_data.xs(username)
	# date_indices = np.unique(comm_days_line_chart_data.index.get_level_values('date'))
	date_indices = user_xs.index
	for date in date_indices:
		if date in comm_df.index:
			comm_days_line_chart_data.loc[(username, date), cols] = comm_df.loc[date,cols]
	return comm_days_line_chart_data


def comm_vol_line_chart(comm_df, comm_vol_line_chart_data, username):
	# TODO: check values
	cols = comm_vol_line_chart_data.columns
	user_xs = comm_vol_line_chart_data.xs(username)
	# date_indices = np.unique(comm_vol_line_chart_data.index.get_level_values('date'))
	date_indices = user_xs.index
	for date in date_indices:
		if date in comm_df.index:
			comm_vol_line_chart_data.loc[(username, date), cols] = comm_df.loc[date,cols]
	return comm_vol_line_chart_data


def generate_report_variables(username, report_variables, comm_df, date_indices):
	report_date = max(date_indices).date()
	today = dt.date.today()

	avg_weekly_total_comm = comm_df['total_comm'][min(date_indices).date():report_date].mean()
	avg_weekly_risky_comm = comm_df['risky_comm'][min(date_indices).date():report_date].mean()
	print(comm_df.index)
	print(report_date)
	if report_date in comm_df.index:
		day_with_max_total_comm = comm_df.loc[report_date, 'high_total_comm_day']
		day_with_max_risky_comm = comm_df.loc[report_date, 'high_risky_comm_day']
		change_in_risky_comm = comm_df['change_in_risky_comm'][report_date]
		change_in_risky_comm = '{:.0%}'.format(change_in_risky_comm)
		change_in_risky_comm_days = comm_df['change_in_risky_comm_days'][report_date]
	else:
		day_with_max_total_comm = np.nan
		day_with_max_risky_comm = np.nan
		change_in_risky_comm = np.nan
		change_in_risky_comm_days = np.nan

	# change_in_risky_loc_days = comm_df['change_in_days_w_risky_loc_visits'][date_indicies[-2]]
	# day_with_max_risky_loc = users[e]['weekly_loc_visits']['high_risky_loc_visits_day'][date_indicies[-2]]
	# known_risky_loc = users[e]['summary_stats']['number_of_risky_locations']

	report_variables[username] = {"title": "HeyCharlie User Report: " + username + " " + today.strftime('%b %d %Y'),
								"user_name": username,
								"clinic_ID": 'Genesis Counseling Services',
								"report_date": report_date.strftime('%b %d %Y'),
								"report_week": report_date.strftime('%b %d'),
								"change_in_risky_comm": change_in_risky_comm,
								"change_in_risky_comm_days": '{:+.0f}'.format(change_in_risky_comm_days) + " Days",
								# "change_in_risky_loc_days": '{:+.0f}'.format(change_in_risky_loc_days) + " Days",
								"avg_weekly_total_comm": '{:.0f}'.format(avg_weekly_total_comm),
								"avg_weekly_risky_comm": '{:.0f}'.format(avg_weekly_risky_comm),
								"day_with_max_total_comm": day_with_max_total_comm,
								"day_with_max_risky_comm": day_with_max_risky_comm}
								# "day_with_max_risky_loc": day_with_max_risky_loc,
								# "known_risky_loc": '{:.0f}'.format(known_risky_loc)}
	return report_variables


def generate_report_data(usernames, date_indices, PROJ_ROOT):
	# Get DataSets:
	# 	Check that the datasets exist, else call for them
	interim_data_path = os.path.join(PROJ_ROOT,
									"data",
									"interim")
	report_data_path = os.path.join(PROJ_ROOT,
									"reports",
									"report_variables")
	multi_index = pd.MultiIndex.from_product([usernames, date_indices], names=['usernames', 'date'])

	comm_pie_chart_cols = ['risky_percent', 'neutral_percent', 'supportive_percent', 'unrated_percent']
	comm_pie_chart_data = pd.DataFrame(np.nan, index=usernames, columns=comm_pie_chart_cols)
	comm_days_line_chart_cols = ['total_comm_days', 'risky_comm_days', 'supportive_comm_days']
	comm_days_line_chart_data = pd.DataFrame(np.nan, index=multi_index, columns=comm_days_line_chart_cols)
	comm_vol_line_chart_cols = ['total_comm', 'risky_comm', 'neutral_comm', 'supportive_comm', 'unrated_comm']
	comm_vol_line_chart_data = pd.DataFrame(np.nan, index=multi_index, columns=comm_vol_line_chart_cols)
	report_variables = {}

	report_date = max(date_indices).date()

	for username in usernames:
		interim_data_file_path = os.path.join(interim_data_path, 'week_comm_log_df_' + username + '.pkl')
		weekly_comm_df = pd.read_pickle(interim_data_file_path)
		print(weekly_comm_df.columns)

		comm_pie_chart_data = comm_pie_chart(weekly_comm_df, comm_pie_chart_data, username, report_date - dt.timedelta(7))
		comm_pie_chart_data.to_pickle(os.path.join(report_data_path + 'comm_pie_chart_data.pkl'))


		comm_days_line_chart_data = comm_days_line_chart(weekly_comm_df, comm_days_line_chart_data, username)
		comm_days_line_chart_data.to_pickle(os.path.join(report_data_path + 'comm_days_line_chart_data.pkl'))


		comm_vol_line_chart_data = comm_vol_line_chart(weekly_comm_df, comm_vol_line_chart_data, username)
		comm_vol_line_chart_data.to_pickle(os.path.join(report_data_path + 'comm_vol_line_chart_data.pkl'))


		report_variables = generate_report_variables(username, report_variables, weekly_comm_df, date_indices)
		# TODO: write report variables to file

	print(comm_vol_line_chart_data.index)
	print(comm_vol_line_chart_data)
	print(report_variables)
	return comm_pie_chart_data, comm_days_line_chart_data, comm_vol_line_chart_data, report_variables
