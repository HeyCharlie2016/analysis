import os
import pandas as pd
import datetime as dt

import database_query
import user_df_setup
import contacts_df_setup
import comm_df_setup


def check_users(usernames, interim_data_file_path):
    try:
        users_df = pd.read_pickle(interim_data_file_path)
    except:
        return usernames
    yesterday = dt.date.today() - dt.timedelta(1)
    print(users_df.index)
    users_needing_updating = list(set(usernames) - set(users_df.index))
    print(users_needing_updating)
    users_with_data = list(set(usernames) & set(users_df.index))
    for e in users_with_data:
        # TODO: update the criteria for what needs to be updated, based on what date range is being looked at
        if users_df['refresh_time'][e] < yesterday:
            users_needing_updating.append(e)
    return users_needing_updating


def refresh_user_data(usernames):

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
    # TODO retain some functionality for pulling all the dataset: force for these, and force pull all
    # TODO some cleaning step here in case the usernames don't exist anywhere
    usernames = check_users(usernames, os.path.join(interim_data_path, 'users_df.pkl'))
    if len(usernames) == 0:
        print('All up to date!')
        return True
    print("updating data for:")
    print(usernames)
    database_query.pull_raw_data(usernames, raw_data_path)

    users_df = user_df_setup.user_df_setup(usernames,
                                           os.path.join(raw_data_path, 'users_df.pkl'),
                                           os.path.join(interim_data_path, 'users_df.pkl'))
    for username in usernames:
        contacts_df = contacts_df_setup.contacts_df_setup(username, users_df, raw_data_path, interim_data_path)

        comm_df = comm_df_setup.create_interim_comm_data(username, users_df, contacts_df, raw_data_path,
                                                         interim_data_path)
        comm_df_setup.sort_daily_comm(username, users_df, comm_df, interim_data_path)
        # TODO Mirror functions for locations (TBD, testing architecture for downstream issues)
