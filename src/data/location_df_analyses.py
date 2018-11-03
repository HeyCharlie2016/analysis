import pandas as pd
import datetime as dt
import numpy as np
import os

# # It doesn't want to import utils...
# from data.utils import add_weekly_highest_day
# from data.utils import add_days_change
# from data.utils import add_change_values

import sys
# add the 'src' directory as one where we can import modules
PROJ_ROOT = os.path.join(__file__,
						 os.pardir,
						 os.pardir,
						 os.pardir)
src_dir = os.path.join(PROJ_ROOT, "src")
sys.path.append(src_dir)
from data import utils


def remove_duplicate_entries(user_loc_activity):
	# Remove duplicate entries with same timestamp and loction
	# TODO this is dropping all the duplicates, not just half of them
	duplicates = []
	for i in user_loc_activity.index:
		result = user_loc_activity[user_loc_activity.index == i]
		if len(result) > 1:
			if i not in duplicates:
				duplicates.append(i)
			user_loc_activity = user_loc_activity.drop(i)
	return user_loc_activity


def add_weekly_totals(daily_loc_df, weekly_loc_df):

	columns = ['safe_loc_visits', 'risky_loc_visits', 'total_loc_visits']
	activity_df = pd.DataFrame(np.nan, index=weekly_loc_df.index, columns=columns)

	for i in columns:
		col_name = 'days_w_' + i
		data = daily_loc_df[[i]] > 0
		temp = data.groupby(pd.cut(data.index, activity_df.index, right=False)).agg({i: pd.Series.sum})
		temp.columns = [col_name]
		temp = temp.reset_index()
		temp.index = temp['index'].apply(lambda x: x.left)
		# print(data)
		# print(weekly_loc_df.index)
		# print(temp[col_name])
		# print(activity_df)
		temp[col_name] *= 1
		activity_df[col_name] = temp[col_name]
	activity_df = activity_df.fillna(0)
	for i in columns:
		col_name = 'days_w_' + i
		weekly_loc_df[col_name] = activity_df[col_name]
	# print(daily_loc_df)
	# print(weekly_loc_df[['risky_loc_visits', 'days_w_risky_loc_visits']])
	return weekly_loc_df.fillna(0)


def add_change_values(weekly_loc_df):
	current_risky_visits = weekly_loc_df['risky_loc_visits'][1:]
	prev_risky_visits = weekly_loc_df['risky_loc_visits'].shift(1)
	weekly_loc_df['change_in_risky_loc_visits'] = (current_risky_visits - prev_risky_visits)

	current_days_w_risky_visits = weekly_loc_df['days_w_risky_loc_visits'][1:]
	prev_days_w_risky_visits = weekly_loc_df['days_w_risky_loc_visits'].shift(1)
	weekly_loc_df['change_in_days_w_risky_loc_visits'] = (current_days_w_risky_visits - prev_days_w_risky_visits)
	return weekly_loc_df


def pull_location_visits(username, user_loc_activity):
	activity = user_loc_activity.sort_values('timestamp', ascending=True)
	#     print(activity.columns)
	shifed_activity = activity.shift(-1)
	location_visits = {}
	in_location = False
	start_index = ''
	time_difference = dt.timedelta(minutes=1000)
	for c, i in enumerate(activity.index):
		if not in_location:
			location_visits[i] = {'locationId': '', 'start_time': '', 'end_time': '', 'risk_type': ''}
			location_visits[i]['locationId'] = activity['locationId'][i]
			location_visits[i]['risk_type'] = activity['risk_type'][i]
			start_index = i
			location_visits[i]['start_time'] = start_index
			in_location = True
		if in_location:
			#             milisecond_difference = shifed_activity_time[i] - activity['timestamp'][i]
			next_timestamp = shifed_activity['timestamp'][i]
			if not np.isnan(np.sum(next_timestamp)):
				next_time = pd.to_datetime(next_timestamp, unit="ms") - dt.timedelta(hours=4)
				time_difference = next_time - i

			# Conditional does the inverse of:
			# if there is a next entry
			# and if the time difference between current and next is relatively small
			# and if the locationID stays the same
			if (time_difference > dt.timedelta(minutes=20)) | np.isnan(np.sum(next_timestamp)) | (
					shifed_activity['locationId'][i] != activity['locationId'][i]):
				location_visits[start_index]['end_time'] = i
				in_location = False
	return pd.DataFrame.from_dict(location_visits, orient='index')


def time_bucket_visits(username, users_df, location_visits_df, interim_data_path, period):
	today = dt.date.today()
	date_created = users_df.loc[username, 'date_created']
	if period == 'day':
		date_indices = pd.date_range(min(date_created, today - dt.timedelta(7)), today + dt.timedelta(7), freq='D')
	elif period == 'week':
		date_indices = pd.date_range(min(date_created, today - dt.timedelta(7)), today + dt.timedelta(7), freq='W-MON')
		# location_visits_df = location_visits_df.fillna(0)
	# TODO: Assertion or something since this is user input?

	columns = ['safe_loc_visits', 'risky_loc_visits', 'total_loc_visits']
	activity_df = pd.DataFrame(np.nan, date_indices, columns=columns)

	if len(location_visits_df) == 0:
		activity_df = {}
	else:
		for i in ['safe', 'risky', 'total']:
			col_name = i + '_loc_visits'
			data = location_visits_df
			if i != 'total':
				data = data[data['risk_type'] == i]
			temp = data.groupby(pd.cut(data.index, activity_df.index, right=False)).agg(
				{'locationId': pd.Series.count})
			temp.columns = [col_name]
			temp = temp.reset_index()
			temp.index = temp['index'].apply(lambda x: x.left)
			activity_df[col_name] = temp[col_name]
	period_loc_visits_df = activity_df.fillna(0)
	# print(username)
	# print(period_loc_visits_df.head())
	# TODO: reduce how often the datafile is being over written
	interim_data_file_path = os.path.join(interim_data_path, period + '_loc_log_df_' + username + '.pkl')
	period_loc_visits_df.to_pickle(interim_data_file_path)
	return period_loc_visits_df


def create_interim_loc_data(username, users_df, user_locations_df, raw_data_path, interim_data_path):
	user_id = users_df.loc[username, 'userId']
	raw_data_file_path = os.path.join(raw_data_path, 'location_log_df_' + user_id + '.pkl')
	raw_loc_log_df = pd.read_pickle(raw_data_file_path)

	user_loc_activity = raw_loc_log_df[raw_loc_log_df['userId'] == user_id]  # risk_scores = pd.Series(np.empty(len(user_activity.index)), index=user_activity.index)
	user_loc_activity = user_loc_activity[['locationId', 'timestamp']]
	user_loc_activity.index = pd.to_datetime(user_loc_activity['timestamp'], unit="ms") - dt.timedelta(hours=4)

	user_loc_activity = remove_duplicate_entries(user_loc_activity)

	# Pull risk type from location log
	if len(user_loc_activity) > 0:
		risk_types = pd.DataFrame(np.nan, index=user_loc_activity.index, columns=['type'])
		for i in user_loc_activity.index:
			location_id = user_loc_activity['locationId'][i]
			risk_types['type'] = user_locations_df.loc[location_id]['type']
			user_loc_activity['risk_type'] = risk_types

	location_visits_df = pull_location_visits(username, user_loc_activity)
	interim_data_file_path = os.path.join(interim_data_path, 'loc_log_df_' + username + '.pkl')
	location_visits_df.to_pickle(interim_data_file_path)
	return location_visits_df


def location_df_setup(username, users_df, locations_df, raw_data_path, interim_data_path):
	location_visits_df = create_interim_loc_data(username, users_df, locations_df, raw_data_path, interim_data_path)
	daily_loc_log_df = time_bucket_visits(username, users_df, location_visits_df, interim_data_path, 'day')
	weekly_loc_log_df = time_bucket_visits(username, users_df, location_visits_df, interim_data_path, 'week')

	# These functions don't write to files
	weekly_loc_log_df = add_weekly_totals(daily_loc_log_df, weekly_loc_log_df)
	weekly_loc_log_df = utils.add_weekly_highest_day(daily_loc_log_df,
													weekly_loc_log_df,
													['risky_loc_visits', 'total_loc_visits'])
	weekly_loc_log_df = utils.add_change_values(weekly_loc_log_df, ['risky_loc_visits', 'days_w_risky_loc_visits'])
	interim_data_file_path = os.path.join(interim_data_path, 'week_loc_log_df_' + username + '.pkl')
	weekly_loc_log_df.to_pickle(interim_data_file_path)
	# print(weekly_loc_log_df.head())
	#
	# print(username)
	# print(weekly_loc_log_df['days_w_risky_loc_visits'])
