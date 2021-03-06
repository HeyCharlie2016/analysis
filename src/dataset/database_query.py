from pymongo import MongoClient
import pandas as pd
import datetime as dt
import os
import numpy as np


def mongo_connect():
	client = MongoClient("mongodb+srv://ben:heycpass@cluster0-wszdu.mongodb.net/admin")
	return client.heycharlie


def get_all_usernames(users_df):
	return users_df['username']


def get_user_ids(users_df, usernames):
	user_ids = []
	missing_usernames = []
	for e in usernames:
		# print(e)
		# if e in users_df['username'].values:
		if e in users_df['username'].values:
			user_ids.append(users_df[users_df['username'] == e]['_id'].values[0].decode())
		else:
			missing_usernames.append(e)
	if missing_usernames:
		print('Username not found: ' + str(missing_usernames))
	usernames = list(set(usernames) - set(missing_usernames))
	return user_ids, usernames


def make_raw_users_df(db, data_file_path):
	# Pulling the current username log, we shouldn't need to worry about filesize for this one but can change later
	users_df = pd.DataFrame(list(db.users.find()))
	# users_df = pd.DataFrame(list(db.users.find({'username': {'$in': usernames}})))
	users_df['_id'] = users_df['_id'].astype('|S')
	users_df.to_pickle(data_file_path)
	return users_df


def update_raw_users_df(db, data_file_path, usernames):
	existing_raw_users_df = pd.read_pickle(data_file_path)

	# current_users_df = pd.DataFrame(list(db.users.find({'username': {'$in': usernames}})))
	users_df = pd.DataFrame(list(db.users.find()))
	users_df['_id'] = users_df['_id'].astype('|S')
	current_users_df = users_df[users_df['username'].isin(usernames)]
	raw_users_df = existing_raw_users_df.combine_first(current_users_df)  # The union of the two, without duplicates
	raw_users_df.to_pickle(data_file_path)
	return raw_users_df


def raw_users_updater(db, usernames, raw_data_path):
	raw_data_file_path = os.path.join(raw_data_path, 'users_df.pkl')
	if os.path.isfile(raw_data_file_path):
		raw_users_df = update_raw_users_df(db, os.path.join(raw_data_path, 'users_df.pkl'), usernames)
	else:
		raw_users_df = make_raw_users_df(db, os.path.join(raw_data_path, 'users_df.pkl'))
	# try:
	# 	# Check for if the file exists
	# 	raw_users_df = update_raw_users_df(db, os.path.join(raw_data_path, 'users_df.pkl'), usernames)
	# except:
	# 	print('Creating new users_df')
	# 	raw_users_df = make_raw_users_df(db, os.path.join(raw_data_path, 'users_df.pkl'))
	return raw_users_df


def make_raw_contacts_df(db, raw_data_path, user_ids):
	try:
		assert len(user_ids) > 0
	except AssertionError:
		print('Empty user_ids list')

	contacts_df = pd.DataFrame(list(db.socialContact.find({'userId': {'$in': user_ids}})))
	contacts_df['_id'] = contacts_df['_id'].astype('|S')
	contacts_df.set_index('_id')

	for e in user_ids:
		user_contacts_df = contacts_df[contacts_df['userId'] == e]
		data_file_path = os.path.join(raw_data_path, 'contacts_df_' + e + '.pkl')
		user_contacts_df.to_pickle(data_file_path)


def make_raw_questionnaire_df(db, raw_data_path, user_ids):
	try:
		assert len(user_ids) > 0
	except AssertionError:
		print('Empty user_ids list')

	questionnaire_df = pd.DataFrame(list(db.contactQuestionnaire.find({'userId': {'$in': user_ids}})))
	if len(questionnaire_df) == 0:
		return
	questionnaire_df['_id'] = questionnaire_df['_id'].astype('|S')
	questionnaire_df.set_index('_id')

	for e in user_ids:
		user_questionnaires_df = questionnaire_df[questionnaire_df['userId'] == e]
		data_file_path = os.path.join(raw_data_path, 'questionnaire_df_' + e + '.pkl')
		user_questionnaires_df.to_pickle(data_file_path)


def make_raw_comm_log_df(db, raw_data_path, user_ids):
	comm_log_df = pd.DataFrame(list(db.contactLog.find({'userId': {'$in': user_ids}})))
	if len(comm_log_df) > 0:
		comm_log_df['timestamp'] = pd.to_datetime(comm_log_df['timestamp'], unit="ms") - dt.timedelta(hours=4)
		comm_log_df.set_index('timestamp')
	else:
		cols = ['userId', 'contactId', 'timestamp', 'direction']
		comm_log_df = pd.DataFrame('', index=[], columns=cols)
	for e in user_ids:
		user_comm_log_df = comm_log_df[comm_log_df['userId'] == e]
		data_file_path = os.path.join(raw_data_path, 'comm_log_df_' + e + '.pkl')
		user_comm_log_df.to_pickle(data_file_path)


def make_raw_location_df(db, raw_data_path, user_ids):
	locations_df = pd.DataFrame(list(db.geographicLocation.find({'userId': {'$in': user_ids}})))
	if len(locations_df) > 0:
		locations_df['_id'] = locations_df['_id'].astype('|S')
	else:
		locations_df = pd.DataFrame('', index=[], columns=['_id', 'type', 'userId'])
	for e in user_ids:
		user_locations_df = locations_df[locations_df['userId'] == e]
		data_file_path = os.path.join(raw_data_path, 'locations_df_' + e + '.pkl')
		user_locations_df.to_pickle(data_file_path)
	# else:
	# 	return False


def make_raw_location_log_df(db, raw_data_path, user_ids):
	location_log_df = pd.DataFrame(list(db.locationLog.find({'userId': {'$in': user_ids}})))
	if len(location_log_df) > 0:
		location_log_df.set_index('_id')
	else:
		cols = ['userId', 'locationId', 'timestamp', 'type']
		location_log_df = pd.DataFrame('', index=[], columns=cols)
	for e in user_ids:
		user_location_log_df = location_log_df[location_log_df['userId'] == e]
		data_file_path = os.path.join(raw_data_path, 'location_log_df_' + e + '.pkl')
		user_location_log_df.to_pickle(data_file_path)


def make_raw_notifications_df(db, raw_data_path, user_ids):
	notifications_df = pd.DataFrame(list(db.notificationLog.find({'userId': {'$in': user_ids}})))
	if len(notifications_df) > 0:
		notifications_df.set_index('_id')
	else:
		cols = ['userId', 'objectId', 'objectType', 'timestamp', 'type']
		notifications_df = pd.DataFrame('', index=[], columns=cols)
	for e in user_ids:
		user_notifications_df = notifications_df[notifications_df['userId'] == e]
		user_notifications_df.index = notifications_df[notifications_df['userId'] == e].index
		data_file_path = os.path.join(raw_data_path, 'notifications_df_' + e + '.pkl')
		user_notifications_df.to_pickle(data_file_path)


def pull_raw_data(usernames, raw_data_path):
	db = mongo_connect()
	raw_users_df = raw_users_updater(db, usernames, raw_data_path)
	[user_ids, updated_usernames] = get_user_ids(raw_users_df, usernames)  # Checks if each exists, and returns a list of those who do
	try:
		assert len(user_ids) == len(updated_usernames)
	except AssertionError:
		print('User_ID Issue')
	# Pulls all the other data for the desired usernames
	# TODO handle if something's empty
	if len(user_ids) > 0:
		make_raw_contacts_df(db, raw_data_path, user_ids)
		make_raw_comm_log_df(db, raw_data_path, user_ids)
		make_raw_location_df(db, raw_data_path, user_ids)
		make_raw_location_log_df(db, raw_data_path, user_ids)
		make_raw_questionnaire_df(db, raw_data_path, user_ids)
		make_raw_notifications_df(db, raw_data_path, user_ids)
		print('Updated raw data for users:')
		print(updated_usernames)

	return updated_usernames
