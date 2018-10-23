import pandas as pd
import datetime as dt
import numpy as np


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

def user_df_setup(raw_data_path, interim_data_path):

	raw_users_df = pd.read_pickle(raw_data_path)

	# desired_usernames = get_desired_usernames(raw_users_df)
	users = pd.DataFrame(np.nan, index=desired_usernames, columns=['date_created'])	

	for e in desired_usernames:
		users.loc[e, 'userId'] = raw_users_df.loc[raw_users_df['username'] == e]._id.values[0].decode()
		dt64 = raw_users_df.loc[raw_users_df['username'] == e]['timeCreated'].values[0]
		date_time = dt.datetime.utcfromtimestamp(dt64.astype('O') / 1e9)
		users.loc[e, 'date_created'] = date_time.date()

		for i in ['unrated', 'risky', 'supportive']:
			col = i + '_threshold'
			users.loc[e, col] = generate_risk_thresholds(i)

	users.to_pickle(interim_data_path)

	return users
