import pandas as pd
import os


def notifications_df_setup(username, users_df, raw_data_path, interim_data_path):
	user_id = users_df.loc[username, 'userId']
	print(user_id)
	raw_data_file_path = os.path.join(raw_data_path, 'notifications_df_' + user_id + '.pkl')
	raw_notifications_df = pd.read_pickle(raw_data_file_path)

	if user_id in raw_notifications_df["userId"].values:
		notifications_df = raw_notifications_df[raw_notifications_df["userId"].values == user_id][['timestamp', 'type', 'userId']]
		# notifications_df['username'] =
		# notifications_df.drop(columns='_id')
		# print(raw_notifications_df.head())
		interim_data_file_path = os.path.join(interim_data_path, 'notifications_df_' + username + '.pkl')
		notifications_df.to_pickle(interim_data_file_path)
		print(notifications_df.head())
		return notifications_df
	else:
		return False
