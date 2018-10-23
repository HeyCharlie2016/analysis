from pymongo import MongoClient
import pandas as pd
import datetime as dt
import os


def mongo_connect():
	client = MongoClient("mongodb+srv://ben:heycpass@cluster0-wszdu.mongodb.net/admin")
	return client.heycharlie


def get_desired_usernames(users_df):
	# recent_users = raw_users_df[['timeCreated','_id','username']].sort_values('timeCreated', ascending = False)
	# recent_users.username.str.encode('utf-8')

	# desired_users = []
	# for e in recent_users['username']:
	# 	desired_users.append(e.encode("utf-8"))
	# # desired_users['username'] = desired_users['username'].encode("utf-8")
	# return desired_users[:10]
	# return recent_users['username'].head(10)

	return ['liamkl', 'vinoct8', 'vinoct2', 'emily2', 'zombeck']
	# return users_df['username'] #this pulls all usernames
# TODO: make a csv - read function for desired usernames

# TODO: function (new file) that checks when the file was last saved and skip it somehow

def get_user_ids(users_df, usernames):
	user_ids = []
	for e in usernames:
		user_ids.append(users_df[users_df['username'] == e]['_id'].values[0].decode())
	return user_ids


def make_raw_users_df(db, data_file_path):
	# For now, pulling all usernames, filesize is small
	users_df = pd.DataFrame(list(db.users.find()))
	users_df['_id'] = users_df['_id'].astype('|S')
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


def pull_raw_data(raw_data_path):
	db = mongo_connect()
	users_df = make_raw_users_df(db, os.path.join(raw_data_path, 'users_df.pkl'))
	# This is where the usernames are established
	usernames = get_desired_usernames(users_df)
	user_ids = get_user_ids(users_df, usernames)

	make_raw_contacts_df(db, raw_data_path, user_ids)
	make_raw_comm_log_df(db, raw_data_path, user_ids)
	make_raw_location_df(db, raw_data_path, user_ids)
	make_raw_location_log_df(db, raw_data_path, user_ids)

	return usernames