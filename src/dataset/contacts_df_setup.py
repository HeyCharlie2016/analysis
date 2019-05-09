import os
import sys
import pandas as pd
import numpy as np

# add the 'src' directory as one where we can import modules
PROJ_ROOT = os.path.join(__file__,
                         os.pardir,
                         os.pardir,
                         os.pardir)
src_dir = os.path.join(PROJ_ROOT, "src")
sys.path.append(src_dir)
from dataset import database_query


def contacts_df_setup(username, users_df, raw_data_path, interim_data_path):
	user_id = users_df.loc[username, 'userId']
	raw_data_file_path = os.path.join(raw_data_path, 'contacts_df_' + user_id + '.pkl')
	raw_contacts_df = pd.read_pickle(raw_data_file_path)

	contacts = raw_contacts_df[raw_contacts_df["userId"] == user_id][['_id', 'score', 'relationship']]
	contacts.index = contacts['_id'].str.decode("utf-8")
	contacts = contacts.drop(columns='_id')
	contacts = pull_contact_questionnaires(username, user_id, contacts, raw_data_path, interim_data_path)

	interim_data_file_path = os.path.join(interim_data_path, 'contacts_df_' + username + '.pkl')
	contacts.to_pickle(interim_data_file_path)
	return contacts


def pull_contact_questionnaires(username, user_id, contacts, raw_data_path, interim_data_path):
	raw_data_file_path = os.path.join(raw_data_path, 'questionnaire_df_' + user_id + '.pkl')

	# if it's a file
	if os.path.isfile(raw_data_file_path):
		raw_questionnaires_df = pd.read_pickle(raw_data_file_path)
	else:
		print('raw_questionnaire data not found, generating new')
		database_query.pull_raw_data([username], raw_data_path)
		raw_questionnaires_df = pd.read_pickle(raw_data_file_path)
		# users_df.index.names = ['username']
	# pull_contact_questionnaire(username, user_id, contacts, raw_data_path, interim_data_path):
	question_numbers = np.arange(1, 9)
	questions = []
	for e in question_numbers:
		questions.append("question " + str(e))

	# raw_questionnaires_df = pd.read_pickle(raw_data_file_path)

	if len(raw_questionnaires_df) == 0:
		# the thing
		for i in questions:
			for index, row in contacts.iterrows():
				contacts.loc[index, i] = np.nan
	else:
		for index, row in contacts.iterrows():
			contact_id = index
			questionnaires = raw_questionnaires_df[raw_questionnaires_df["contactId"] == contact_id]
			if len(questionnaires) == 0:
				pass
			# do the thing
			else:
				current_questionnaire = questionnaires.loc[questionnaires['timestamp'].idxmax()]
				#                 print(current_questionnaire['questions'])
				for e in current_questionnaire['questions']:
					#                     print(e)
					question_id = "question " + str(e['questionId'])
					contacts.loc[contact_id, question_id] = e['answer']

	#         print(contacts)
	# interim_data_file_path = os.path.join(interim_data_path, 'contacts_df_' + username + '.pkl')
	# contacts.to_pickle(interim_data_file_path)
	return contacts
