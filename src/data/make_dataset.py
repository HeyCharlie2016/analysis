import os
import pandas as pd
import numpy as np
import datetime as dt

import sys
# add the 'src' directory as one where we can import modules
PROJ_ROOT = os.path.join(__file__,
                         os.pardir,
                         os.pardir,
                         os.pardir)
src_dir = os.path.join(PROJ_ROOT, "src")
sys.path.append(src_dir)

from data import database_query
from data import user_df_setup
from data import contacts_df_setup
from data import comm_df_analyses
from data import locations_df_setup
from data import location_df_analyses


def check_interim_data(usernames, max_date, interim_data_file_path, positives):
    # This is effectively checking if the data exists and is current.
    # Returns a list of usernames that need to be updated
    # Should probably add in checks for raw data files

    # Currently, the raw users_df.pkl is storing all the users in the db
    #   the interim users_df.pkl has only entries for those with updated interim data files

    try:
        # Checking that the file exists
        users_df = pd.read_pickle(interim_data_file_path)
        print("Interim users_df datafile exists")
    except:
        return usernames

    users_needing_updating = list(set(usernames) - set(users_df.index)) # Users that don't exist
    users_with_data = list(set(usernames) & set(users_df.index)) # Users that do exist (inverse of previous line)

    for e in users_with_data:
        if pd.isnull(users_df['refresh_time'][e]):
            users_needing_updating.append(e)
            users_with_data.remove(e)
        elif (users_df['refresh_time'][e] < max_date):
            users_needing_updating.append(e)
            users_with_data.remove(e)
    if positives == 1:
        return users_with_data
    else:
        return users_needing_updating


def refresh_user_data(usernames, PROJ_ROOT, max_date):

    raw_data_path = os.path.join(PROJ_ROOT,
                                 "data",
                                 "raw")
    interim_data_path = os.path.join(PROJ_ROOT,
                                     "data",
                                     "interim")
    # TODO retain some functionality for pulling all the dataset: force re-pull for the list, and force re-pull all
    usernames_to_update = check_interim_data(usernames,
                                             max_date,
                                             os.path.join(interim_data_path, 'users_df.pkl'),
                                             0)
    if len(usernames_to_update) == 0:
        print('Data is up to date!')
        return usernames
    else:
        print("updating data for:")
        print(usernames_to_update)
        updated_usernames = database_query.pull_raw_data(usernames_to_update, raw_data_path)
        if not updated_usernames:
            print('No data updated, check input usernames')
        else:
            print("Updated data for:")
            print(updated_usernames)
        users_df = user_df_setup.user_df_setup(updated_usernames,
                                           os.path.join(raw_data_path, 'users_df.pkl'),
                                           os.path.join(interim_data_path, 'users_df.pkl'))
    for username in updated_usernames:
        contacts_df = contacts_df_setup.contacts_df_setup(username, users_df, raw_data_path, interim_data_path)

        comm_df_analyses.comm_df_setup(username, users_df, contacts_df, raw_data_path,
                                                         interim_data_path)
        locations_df = locations_df_setup.locations_df_setup(username, users_df, raw_data_path, interim_data_path)
        location_df_analyses.location_df_setup(username, users_df, locations_df, raw_data_path,
                                                         interim_data_path)

    # Confirm update
    users_df = user_df_setup.mark_refreshed(updated_usernames, users_df, os.path.join(interim_data_path, 'users_df.pkl'))
    usernames = check_interim_data(usernames, max_date, os.path.join(interim_data_path, 'users_df.pkl'), 1)
    print('Dataset current for:')
    print(usernames)
    return usernames
