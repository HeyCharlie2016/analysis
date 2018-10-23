import os

import database_query
import user_df_setup
import contacts_df_setup
import comm_df_setup


if __name__ == '__main__':
    PROJ_ROOT = os.path.join(__file__,
                             os.pardir,
                             os.pardir,
                             os.pardir)

    PROJ_ROOT = os.path.abspath(PROJ_ROOT)

    raw_data_path = os.path.join(PROJ_ROOT,
                                 "data",
                                 "raw")

    # TODO This is where we'll insert a list of desired usernames
    usernames = database_query.pull_raw_data(raw_data_path)

    interim_data_path = os.path.join(PROJ_ROOT,
                                     "data",
                                     "interim")

    users_df = user_df_setup.user_df_setup(usernames,
                                           os.path.join(raw_data_path, 'users_df.pkl'),
                                           os.path.join(interim_data_path, 'users_df.pkl'))
    for username in usernames:
        contacts_df = contacts_df_setup.contacts_df_setup(username, users_df, raw_data_path, interim_data_path)

        comm_df = comm_df_setup.create_interim_comm_data(username, users_df, contacts_df, raw_data_path,
                                                         interim_data_path)
        comm_df_setup.sort_daily_comm(username, users_df, comm_df, interim_data_path)
