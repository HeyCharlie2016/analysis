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


def comm_vol_line_chart(comm_df, comm_vol_line_chart_data, username, multi_index):
	# cols = [ 'total_comm_days', 'risky_comm_days', 'supportive_comm_days']
	# labels = [ 'Any Comm', 'Risky Comm', 'Supportive Comm']
	# linestyle=['solid', 'dashed', 'dotted']
	cols = comm_vol_line_chart_data.columns
	user_xs = comm_vol_line_chart_data.xs(username)
	# comm_vol_line_chart_data[username] = comm_df.loc[multi_index['date']]
	date_indices = np.unique(comm_vol_line_chart_data.index.get_level_values('date'))
	date_indices = user_xs.index
	# print(date_indices)
	for date in date_indices:
		print('here')
		# print(comm_vol_line_chart_data.loc[(username, date), cols])
		# print(comm_df[date])
		if date in comm_df.index:
			# print(comm_df.loc[date, cols])
			comm_vol_line_chart_data.loc[(username, date), cols] = comm_df.loc[date,cols]
	# for i, c in enumerate(users[e]['weekly_activity'][cols][:-1]
	# print(comm_vol_line_chart_data.xs(username))
	return comm_vol_line_chart_data

# pull individual report variables


def generate_report_data(usernames, date_indices, PROJ_ROOT):
	# Get DataSets:
	# 	Check that the datasets exist, else call for them
	interim_data_path = os.path.join(PROJ_ROOT,
									 "data",
									 "interim")
	multi_index = pd.MultiIndex.from_product([usernames, date_indices], names=['usernames', 'date'])
	# print(multi_index)

	comm_pie_chart_cols = ['risky_percent', 'neutral_percent', 'supportive_percent', 'unrated_percent']
	comm_pie_chart_data = pd.DataFrame(np.nan, index=usernames, columns=comm_pie_chart_cols)
	comm_vol_line_chart_cols = ['username', 'total_comm_days', 'risky_comm_days', 'supportive_comm_days']
	comm_vol_line_chart_data = pd.DataFrame(np.nan, index=multi_index, columns=comm_pie_chart_cols)
	# print(np.unique(comm_vol_line_chart_data.index.get_level_values('date')))

	report_date = max(date_indices).date()
	# print(comm_pie_chart_data)
	for username in usernames:
		interim_data_file_path = os.path.join(interim_data_path, 'week_comm_log_df_' + username + '.pkl')
		weekly_comm_df = pd.read_pickle(interim_data_file_path)

		comm_pie_chart_data = comm_pie_chart(weekly_comm_df, comm_pie_chart_data, username, report_date - dt.timedelta(7))
		comm_vol_line_chart_data = comm_vol_line_chart(weekly_comm_df, comm_vol_line_chart_data, username, multi_index)

	print(comm_vol_line_chart_data.index)
	print(comm_vol_line_chart_data)

	# return chart_data, report_variables