import pandas as pd
import os


def contacts_df_setup(username, users_df, raw_data_path, interim_data_path):
	user_id = users_df.loc[username, 'userId']
	raw_data_file_path = os.path.join(raw_data_path, 'contacts_df_' + user_id + '.pkl')
	raw_contacts_df = pd.read_pickle(raw_data_file_path)

	contacts = raw_contacts_df[raw_contacts_df["userId"] == user_id][['_id', 'score']]
	contacts.index = contacts['_id'].str.decode("utf-8")
	contacts.drop(columns='_id')

	interim_data_file_path = os.path.join(interim_data_path, 'contacts_df_' + username + '.pkl')
	contacts.to_pickle(interim_data_file_path)
	return contacts
