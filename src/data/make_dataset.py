import os
import pandas as pd
import datetime as dt

import database_query
import user_df_setup
import contacts_df_setup
import daily_comm_data

def make_users_df(db, data_file_path):  
    users_df = pd.DataFrame(list(db.users.find()))
    users_df['_id'] = users_df['_id'].astype('|S')
    users_df.to_pickle(data_file_path)
    return users_df

def make_contacts_df(db, data_file_path):   
    contacts_df = pd.DataFrame(list(db.socialContact.find()))
    contacts_df['_id'] = contacts_df['_id'].astype('|S')
    contacts_df.index = contacts_df['_id']
    contacts_df.to_pickle(data_file_path)

def make_comm_log_df(db, data_file_path):   
    comm_log_df = pd.DataFrame(list(db.contactLog.find()))
    comm_log_df.index = pd.to_datetime(comm_log_df['timestamp'], unit = "ms") - dt.timedelta(hours=4)
    comm_log_df.to_pickle(data_file_path)

def make_location_df(db, data_file_path):   
    location_df = pd.DataFrame(list(db.geographicLocation.find()))
    location_df['_id'] = location_df['_id'].astype('|S')
    location_df.to_pickle(data_file_path)

def make_location_log_df(db, data_file_path):   
    location_log_df = pd.DataFrame(list(db.locationLog.find()))
    location_log_df.index = location_log_df['_id']
    location_log_df.to_pickle(data_file_path)

if __name__ == '__main__':
    PROJ_ROOT = os.path.join(__file__,
                             os.pardir,
                             os.pardir,
                             os.pardir)

    PROJ_ROOT = os.path.abspath(PROJ_ROOT)

    raw_data_path = os.path.join(PROJ_ROOT,
                                 "data",
                                 "raw")

    db = database_query.mongo_connect()
    make_users_df(db, os.path.join(raw_data_path, 'users_df.pkl'))
    make_contacts_df(db, os.path.join(raw_data_path, 'contacts_df.pkl'))
    make_comm_log_df(db, os.path.join(raw_data_path, 'comm_log_df.pkl'))
    make_location_df(db, os.path.join(raw_data_path, 'location_df.pkl'))
    make_location_log_df(db, os.path.join(raw_data_path, 'location_log_df.pkl'))

    data_path = os.path.join(PROJ_ROOT,
                                 "data")

    interim_data_path = os.path.join(PROJ_ROOT,
                                 "data",
                                 "interim")

    users_df = user_df_setup.user_df_setup(os.path.join(raw_data_path, 'users_df.pkl'),
                                os.path.join(interim_data_path, 'users_df.pkl'))
    contacts_df = contacts_df_setup.contacts_df_setup(users_df,
                                os.path.join(raw_data_path, 'contacts_df.pkl'),
                                os.path.join(interim_data_path, 'contacts_df.pkl'))


    # daily_comm_data.generate_daily_comm(users_df, os.path.join(interim_data_path, 'daily_comm_df.pkl'))