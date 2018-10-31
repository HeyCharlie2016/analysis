import pandas as pd
import os

# Basically a copy of contacts_df_setup
def locations_df_setup(username, users_df, raw_data_path, interim_data_path):
	user_id = users_df.loc[username, 'userId']
	raw_data_file_path = os.path.join(raw_data_path, 'locations_df_' + user_id + '.pkl')
	raw_locations_df = pd.read_pickle(raw_data_file_path)

	locations = raw_locations_df[raw_locations_df["userId"] == user_id][['_id', 'type']]
	locations.index = locations['_id'].str.decode("utf-8")
	locations.drop(columns='_id')

	interim_data_file_path = os.path.join(interim_data_path, 'locations_df_' + username + '.pkl')
	locations.to_pickle(interim_data_file_path)
	return locations
