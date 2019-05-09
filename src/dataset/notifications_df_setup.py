import pandas as pd
import os
import datetime as dt


def notifications_df_setup(username, users_df, raw_data_path, interim_data_path):
	user_id = users_df.loc[username, 'userId']
	# print(user_id)
	raw_data_file_path = os.path.join(raw_data_path, 'notifications_df_' + user_id + '.pkl')
	raw_notifications_df = pd.read_pickle(raw_data_file_path)
	# print(raw_notifications_df.head())
	if user_id in raw_notifications_df["userId"].values:
		notifications_df = raw_notifications_df[raw_notifications_df["userId"].values == user_id][['userId', 'objectId', 'objectType', 'timestamp', 'type']]
		# TODO notifications_df.index = raw_notifications_df[raw_notifications_df["userId"].values == user_id]['_id']
		notifications_df = notifications_df.drop(columns='userId')
		notifications_df['timestamp'] = pd.to_datetime(notifications_df['timestamp'], unit="ms") - dt.timedelta(hours=4)
		notifications_df.rename(index=str, columns={"objectId": "referenceId", "objectType": "notificationType"})

		# comm_df = comm_df.drop(columns='timestamp')

		# notifications_df['username'] =
		# notifications_df.drop(columns='_id')
		# print(raw_notifications_df.head())
		interim_data_file_path = os.path.join(interim_data_path, 'notifications_log_df_' + username + '.pkl')
		notifications_df.to_pickle(interim_data_file_path)
		# print(notifications_df.head())
		return notifications_df
	else:
		return False
