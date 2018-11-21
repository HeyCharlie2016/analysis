import pandas as pd
import datetime as dt
import numpy as np
import os


# def get_desired_usernames(raw_users_df):
# 	recent_users = raw_users_df[['timeCreated','_id','username']].sort_values('timeCreated', ascending = False)
# 	recent_users.username.str.encode('utf-8')
#
# 	# desired_users = []
# 	# for e in recent_users['username']:
# 	# 	desired_users.append(e.encode("utf-8"))
# 	# # desired_users['username'] = desired_users['username'].encode("utf-8")
# 	# return desired_users[:10]
# 	# return recent_users['username'].head(10)
# 	return ['liamkl', 'vinoct8', 'vinoct2', 'emily2', 'zombeck']


def generate_risk_thresholds(col):
	d = {'unrated': 0,
						'risky': 1,
						'supportive': 3}
	return d[col]


def user_df_setup(raw_data_file_path, interim_data_file_path, *args, ** kwargs):
	usernames = kwargs.get('usernames', None)
	raw_users_df = pd.read_pickle(raw_data_file_path)
	if usernames is None:
		usernames = raw_users_df['username']
	if os.path.isfile(interim_data_file_path):
		users_df = pd.read_pickle(interim_data_file_path)
	else:
		print('Interim users_df not found, generating new')
		users_df = pd.DataFrame(np.nan, index=usernames, columns=['date_created', 'refresh_time'])
		users_df.index.names = ['username']

	# print(usernames)
	# users_needing_entries = list(set(usernames) - set(users_df.index))  # Users that don't exist
	# print(users_needing_entries)
	if usernames is None:
		users_df = raw_users_df[['timeCreated', '_id', 'username']].sort_values('timeCreated', ascending=False)
		# for i in ['unrated', 'risky', 'supportive']:
		# 	col = i + '_threshold'
		# 	users_df[col] = pd.Series(generate_risk_thresholds(i), index=users_df.index)
			# TODO call this from jupyter notebook without a usernames argument
	else:
		for e in usernames:
			# could skip some of this for the existing users and just update the refresh timestamp
			users_df.loc[e, 'userId'] = raw_users_df.loc[raw_users_df['username'] == e]._id.values[0].decode()
			dt64 = raw_users_df.loc[raw_users_df['username'] == e]['timeCreated'].values[0]
			date_time = dt.datetime.utcfromtimestamp(dt64.astype('O') / 1e9)
			users_df.loc[e, 'date_created'] = date_time.date()
			# users_df.loc[e, 'refresh_time'] = 0
			for i in ['unrated', 'risky', 'supportive']:
				col = i + '_threshold'
				users_df.loc[e, col] = generate_risk_thresholds(i)

	users_df.to_pickle(interim_data_file_path)
	# print(users_df.index)
	return users_df


def mark_refreshed(usernames, users_df, interim_data_file_path):
	refresh_time = dt.date.today()
	for e in usernames:
		users_df.loc[e, 'refresh_time'] = refresh_time
	users_df.to_pickle(interim_data_file_path)
	return users_df
