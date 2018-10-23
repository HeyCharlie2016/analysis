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

    # This is where we'll insert a list of desired usernames
    database_query.pull_raw_data(raw_data_path)


    interim_data_path = os.path.join(PROJ_ROOT,
                                        "data",
                                        "interim")

    # start a loop here for each entry on list of desired usernames, rather than having them lop in each file
    users_df = user_df_setup.user_df_setup(os.path.join(raw_data_path, 'users_df.pkl'),
                                os.path.join(interim_data_path, 'users_df.pkl'))
    contacts_df = contacts_df_setup.contacts_df_setup(users_df,
                                os.path.join(raw_data_path, 'contacts_df.pkl'),
                                os.path.join(interim_data_path, 'contacts_df.pkl'))

    comm_df = comm_df_setup.create_interim_comm_data(contacts_df,
                                                     os.path.join(raw_data_path, 'comm_log_df.pkl'),
                                                     os.path.join(interim_data_path, 'Communication_df.pkl'))
    comm_df_setup.sort_daily_comm(users_df, comm_df, os.path.join(interim_data_path, 'daily_comm_df.pkl'))