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

def comm_pie_chart(comm_df, comm_pie_chart_data, date):
	# print(comm_df.type())
	cols = ['risky_percent', 'neutral_percent', 'supportive_percent', 'unrated_percent']
	# currently looks at the second one from the end due to the need for a trailing date while bucketing...
	if date in comm_df.index: # not np.isnan(comm_df['risky_percent'].loc[date]):
		data = comm_df[cols].loc[date]
		print(data)
		comm_pie_chart_data = comm_pie_chart_data.append(data, ignore_index=True)
		return comm_pie_chart_data
	print("no pie chart data")

# pull individual report variables

def generate_report_data(usernames, date_indices, PROJ_ROOT):
	# Get DataSets:
	# 	Check that the datasets exist, else call for them
	interim_data_path = os.path.join(PROJ_ROOT,
									 "data",
									 "interim")
	comm_pie_chart_data = pd.DataFrame(np.nan, index=usernames, columns=['temp'])
	report_date = max(date_indices).date()

	for username in usernames:
		interim_data_file_path = os.path.join(interim_data_path, 'week_comm_log_df_' + username + '.pkl')
		weekly_comm_df = pd.read_pickle(interim_data_file_path)

		comm_pie_chart_data = comm_pie_chart(weekly_comm_df, comm_pie_chart_data, report_date - dt.timedelta(7))
		# comm_pie_chart_data = comm_pie_chart_data.append(new_entry, ignore_index=True)


	print(comm_pie_chart_data)

	# return chart_data, report_variables