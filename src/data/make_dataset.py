import os
import pandas as pd
import datetime as dt

import database_query
import user_df_setup
import contacts_df_setup
import comm_df_setup


def check_users(usernames, max_report_date, interim_data_file_path):
    # This is effectively checking if the data exists and is current.
    # Returns a list of usernames that need to be updated
    # Should probably add in checks for raw data files

    # Currently, the raw users_df.pkl is storing all the users in the db
    #   the interim users_df.pkl has only data for those with updated interim data files

    try:
        # Checking that the file exists
        users_df = pd.read_pickle(interim_data_file_path)
    except:
        return usernames

    users_needing_updating = list(set(usernames) - set(users_df.index)) # Users that don't exist
    users_with_data = list(set(usernames) & set(users_df.index)) # Users that do exist (inverse of previous line)

    for e in users_with_data:
        if users_df['refresh_time'][e] < max_report_date:
            users_needing_updating.append(e)
    return users_needing_updating


def refresh_user_data(usernames, max_report_date):

    PROJ_ROOT = os.path.join(__file__,
                             os.pardir,
                             os.pardir,
                             os.pardir)

    PROJ_ROOT = os.path.abspath(PROJ_ROOT)
    raw_data_path = os.path.join(PROJ_ROOT,
                                 "data",
                                 "raw")
    interim_data_path = os.path.join(PROJ_ROOT,
                                     "data",
                                     "interim")
    # TODO retain some functionality for pulling all the dataset: force re-pull for the list, and force re-pull all
    usernames = check_users(usernames, max_report_date, os.path.join(interim_data_path, 'users_df.pkl'))
    if len(usernames) == 0:
        print('Data is up to date!')
        return True
    else:
        print("updating data for:")
        print(usernames)
        updated_usernames = database_query.pull_raw_data(usernames, raw_data_path)
        if not updated_usernames:
            print('No data updated, check input usernames')
        else:
            print("Updated data for:")
            print(updated_usernames)
        users_df = user_df_setup.user_df_setup(usernames,
                                           os.path.join(raw_data_path, 'users_df.pkl'),
                                           os.path.join(interim_data_path, 'users_df.pkl'))
    for username in usernames:
        contacts_df = contacts_df_setup.contacts_df_setup(username, users_df, raw_data_path, interim_data_path)

        comm_df = comm_df_setup.create_interim_comm_data(username, users_df, contacts_df, raw_data_path,
                                                         interim_data_path)
        comm_df_setup.time_bucket_comm(username, users_df, comm_df, interim_data_path, 'day')
        comm_df_setup.time_bucket_comm(username, users_df, comm_df, interim_data_path, 'week')
        # TODO Mirror functions for locations (TBD, testing architecture for downstream issues)
    print('Data has been updated.')