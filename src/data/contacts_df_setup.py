import pandas as pd
import numpy as np
import pickle

def contacts_df_setup(users, raw_data_path, interim_data_path):
	raw_contacts_df = pd.read_pickle(raw_data_path)
	# print(raw_contacts_df)
	contacts = {}
	# contacts = pd.DataFrame(np.nan, index=users.index, columns=['init_column'])
	# print(contacts)	
	for e in users.index:
		contacts[e] = {}
		contacts[e] = raw_contacts_df[raw_contacts_df["userId"] == users.loc[e, 'userId']][['_id', 'score']]
		contacts[e].index = contacts[e]['_id'].str.decode("utf-8")
		contacts[e].drop(columns='_id')
		# contacts.to_pickle(interim_data_path)
		with open(interim_data_path, 'wb') as fp:
			pickle.dump(contacts, fp, protocol=pickle.HIGHEST_PROTOCOL)
	# print(contacts)
	return contacts