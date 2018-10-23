from pymongo import MongoClient
import pandas as pd
import datetime as dt
import os


def mongo_connect():
    client = MongoClient("mongodb+srv://ben:heycpass@cluster0-wszdu.mongodb.net/admin")
    return client.heycharlie


def make_raw_users_df(db, data_file_path):
    users_df = pd.DataFrame(list(db.users.find()))
    users_df['_id'] = users_df['_id'].astype('|S')
    users_df.to_pickle(data_file_path)


def make_raw_contacts_df(db, data_file_path):
    contacts_df = pd.DataFrame(list(db.socialContact.find()))
    contacts_df['_id'] = contacts_df['_id'].astype('|S')
    contacts_df.index = contacts_df['_id']
    contacts_df.to_pickle(data_file_path)


def make_raw_comm_log_df(db, data_file_path):
    comm_log_df = pd.DataFrame(list(db.contactLog.find()))
    comm_log_df.index = pd.to_datetime(comm_log_df['timestamp'], unit="ms") - dt.timedelta(hours=4)
    comm_log_df.to_pickle(data_file_path)


def make_raw_location_df(db, data_file_path):
    location_df = pd.DataFrame(list(db.geographicLocation.find()))
    location_df['_id'] = location_df['_id'].astype('|S')
    location_df.to_pickle(data_file_path)


def make_raw_location_log_df(db, data_file_path):
    location_log_df = pd.DataFrame(list(db.locationLog.find()))
    location_log_df.index = location_log_df['_id']
    location_log_df.to_pickle(data_file_path)


def pull_raw_data(raw_data_path):
    db = mongo_connect()

    make_raw_users_df(db, os.path.join(raw_data_path, 'users_df.pkl'))
    make_raw_contacts_df(db, os.path.join(raw_data_path, 'contacts_df.pkl'))
    make_raw_comm_log_df(db, os.path.join(raw_data_path, 'comm_log_df.pkl'))
    make_raw_location_df(db, os.path.join(raw_data_path, 'location_df.pkl'))
    make_raw_location_log_df(db, os.path.join(raw_data_path, 'location_log_df.pkl'))