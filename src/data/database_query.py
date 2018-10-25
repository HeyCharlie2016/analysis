from pymongo import MongoClient
import pandas as pd
import datetime as dt
import os


def mongo_connect():
	client = MongoClient("mongodb+srv://ben:heycpass@cluster0-wszdu.mongodb.net/admin")
	return client.heycharlie


def get_all_usernames(users_df):
	return users_df['username']


def get_user_ids(users_df, usernames):
	user_ids = []
	for e in usernames:
		if e not in users_df['username']:
			print('data not found for ' + e)
			usernames.remove(e)
			continue
		user_ids.append(users_df[users_df['username'] == e]['_id'].values[0].decode())
	return user_ids, usernames


def make_raw_users_df(db, data_file_path, usernames):
	# Pulling the current username log, we shouldn't need to worry about filesize for this one but can change later
	users_df = pd.DataFrame(list(db.users.find()))
	# users_df = pd.DataFrame(list(db.users.find({'username': {'$in': usernames}})))
	users_df['_id'] = users_df['_id'].astype('|S')
	users_df.to_pickle(data_file_path)
	return users_df


def update_raw_users_df(db, data_file_path, usernames):
	users_df = pd.read_pickle(data_file_path)
	current_users_df = pd.DataFrame(list(db.users.find({'username': {'$in': usernames}})))
	current_users_df['_id'] = current_users_df['_id'].astype('|S')
	users_df = users_df.combine_first(current_users_df)  # The union of the two, without duplicates
	users_df.to_pickle(data_file_path)
	return users_df


def make_raw_contacts_df(db, raw_data_path, user_ids):
	contacts_df = pd.DataFrame(list(db.socialContact.find({'userId': {'$in': user_ids}})))
	contacts_df['_id'] = contacts_df['_id'].astype('|S')
	contacts_df.index = contacts_df['_id']
	for e in user_ids:
		user_contacts_df = contacts_df[contacts_df['userId'] == e]
		data_file_path = os.path.join(raw_data_path, 'contacts_df_' + e + '.pkl')
		user_contacts_df.to_pickle(data_file_path)


def make_raw_comm_log_df(db, raw_data_path, user_ids):
	comm_log_df = pd.DataFrame(list(db.contactLog.find({'userId': {'$in': user_ids}})))
	comm_log_df.index = pd.to_datetime(comm_log_df['timestamp'], unit="ms") - dt.timedelta(hours=4)
	for e in user_ids:
		user_comm_log_df = comm_log_df[comm_log_df['userId'] == e]
		data_file_path = os.path.join(raw_data_path, 'comm_log_df_' + e + '.pkl')
		user_comm_log_df.to_pickle(data_file_path)


def make_raw_location_df(db, raw_data_path, user_ids):
	locations_df = pd.DataFrame(list(db.geographicLocation.find({'userId': {'$in': user_ids}})))
	locations_df['_id'] = locations_df['_id'].astype('|S')
	for e in user_ids:
		user_locations_df = locations_df[locations_df['userId'] == e]
		data_file_path = os.path.join(raw_data_path, 'locations_df_' + e + '.pkl')
		user_locations_df.to_pickle(data_file_path)


def make_raw_location_log_df(db, raw_data_path, user_ids):
	location_log_df = pd.DataFrame(list(db.locationLog.find({'userId': {'$in': user_ids}})))
	location_log_df.index = location_log_df['_id']
	for e in user_ids:
		user_location_log_df = location_log_df[location_log_df['userId'] == e]
		data_file_path = os.path.join(raw_data_path, 'location_log_df_' + e + '.pkl')
		user_location_log_df.to_pickle(data_file_path)


def pull_raw_data(usernames, raw_data_path):
	db = mongo_connect()
	try:
		# Check for if the file exists
		users_df = update_raw_users_df(db, os.path.join(raw_data_path,'users_df.pkl'), usernames)
	except:
		print('Creating new users_df')
		users_df = make_raw_users_df(db, os.path.join(raw_data_path, 'users_df.pkl'), usernames)

	[user_ids, usernames] = get_user_ids(users_df, usernames)  # Maybe overly complex? Checks if each exists.
	print('updating raw users')
	print(users_df['username'])
	if len(usernames) > 0:
		make_raw_contacts_df(db, raw_data_path, user_ids)
		make_raw_comm_log_df(db, raw_data_path, user_ids)
		make_raw_location_df(db, raw_data_path, user_ids)
		make_raw_location_log_df(db, raw_data_path, user_ids)
	return usernames
