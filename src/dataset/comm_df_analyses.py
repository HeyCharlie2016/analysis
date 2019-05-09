import pandas as pd
import datetime as dt
import numpy as np
import os

# # It doesn't want to import utils...
# from data.utils import add_weekly_highest_day
# from data.utils import add_days_change

import sys
# add the 'src' directory as one where we can import modules
PROJ_ROOT = os.path.join(__file__,
						 os.pardir,
						 os.pardir,
						 os.pardir)
src_dir = os.path.join(PROJ_ROOT, "src")
sys.path.append(src_dir)
from dataset import utils


def comm_activity_columns():
    activity_columns = []
    for j in ['risky', 'neutral', 'supportive', 'unrated']:
        for i in ['sms_sent', 'sms_received', 'phone_inbound', 'phone_outbound']:
            activity_columns.append(i + '_' + j)
    #     activity_columns.append(j + '_comm')
    # activity_columns.append('total_comm')
    return activity_columns


def create_interim_comm_data(username, users_df, contacts_df, raw_data_path, interim_data_path):
    user_id = users_df.loc[username, 'userId']
    raw_data_file_path = os.path.join(raw_data_path, 'comm_log_df_' + user_id + '.pkl')
    raw_comm_df = pd.read_pickle(raw_data_file_path)
    user_activity = raw_comm_df[raw_comm_df['userId'] == user_id]

    # risk_scores = pd.Series(np.empty(len(user_activity.index)), index=user_activity.index)
    risk_scores = pd.DataFrame(np.nan, index=user_activity.index, columns=['risk_score', 'relationship'])
    for index, row in user_activity.iterrows():
        contact_id = row['contactId']
        risk_scores.loc[index, 'risk_score'] = contacts_df.loc[contact_id]['score']
        risk_scores.loc[index, 'relationship'] = contacts_df.loc[contact_id]['relationship']
    user_activity = user_activity.assign(risk_score=risk_scores['risk_score'], relationship=risk_scores['relationship'])
    comm_df = user_activity[['contactId', 'direction', 'timestamp', 'risk_score', 'relationship']]
    comm_df.index = pd.to_datetime(comm_df['timestamp'], unit="ms") - dt.timedelta(hours=4)
    comm_df = comm_df.drop(columns='timestamp')

    interim_data_file_path = os.path.join(interim_data_path, 'comm_log_df_' + username + '.pkl')
    comm_df.to_pickle(interim_data_file_path)
    return comm_df


def add_volume_by_type(comm_activity_df):
    for i in ['risky', 'neutral', 'supportive', 'unrated']:
        for j in ['sms_sent', 'sms_received', 'phone_inbound', 'phone_outbound']:
            col_name = i + '_comm'
            source_name = j + '_' + i
            if col_name not in comm_activity_df.columns:
                comm_activity_df[col_name] = comm_activity_df[source_name]
            else:
                comm_activity_df[col_name] = comm_activity_df[col_name] + \
                                             comm_activity_df[source_name]
    return comm_activity_df


def add_days_change_with_risky_interactions(weekly_comm_df):
    #     Get (# of Days) Change in Days with Risky Interactions
    current_risky_comm_days = weekly_comm_df['risky_comm_days'][1:]
    prev_risky_comm_days = weekly_comm_df['risky_comm_days'].shift(1)
    weekly_comm_df['change_in_risky_comm_days'] = (current_risky_comm_days - prev_risky_comm_days)
    return weekly_comm_df


def add_percent_change_in_risky_interactions(weekly_comm_df):
    #     Get (%) Change in Risky Interactions
    current_risky_comm = weekly_comm_df['risky_comm'][1:]
    prev_risky_comm = weekly_comm_df['risky_comm'].shift(1)
    change_in_risky_comm = (current_risky_comm - prev_risky_comm) / prev_risky_comm
    weekly_comm_df['change_in_risky_comm'] = change_in_risky_comm
    weekly_comm_df.loc[~np.isfinite(weekly_comm_df['change_in_risky_comm']), 'change_in_risky_comm'] = 1
    return weekly_comm_df


def add_percent_comm_for_each_type(weekly_comm_df):
    labels = ['risky', 'neutral', 'supportive', 'unrated']
    for i in labels:
        col_name = i + '_percent'
        source_col = i + '_comm'
        weekly_comm_df[col_name] = weekly_comm_df[source_col] / weekly_comm_df['total_comm']
    return(weekly_comm_df)


def add_days_with_comm_by_type(daily_comm_df, weekly_comm_df):
    labels = ['risky_comm', 'total_comm', 'supportive_comm', 'unrated_comm']
    cols = ['risky_comm_days', 'total_comm_days', 'supportive_comm_days', 'unrated_comm_days']

    # date_indicies = pd.date_range(start, end, freq='7D')
    date_indices = weekly_comm_df.index
    activity_df = pd.DataFrame(np.nan, index=date_indices, columns=cols)

    for i in cols:
        #         temp_data = data.groupby(pd.cut(data.index, activity_df.index, right=False)).transform(max)
        #         temp = data.groupby(pd.cut(data.index, activity_df.index, right=False)).agg({i[:-5] : pd.Series.sum})
        data = daily_comm_df[i[:-5]]  # i[:-5] takes '_days' off the col name for the source col
        temp = data.groupby(pd.cut(data.index, activity_df.index, right=False)).agg(
            [(i[:-5], lambda value: (value > 0).sum())])
        temp.columns = [i]
        temp = temp.reset_index()
        temp.index = temp['index'].apply(lambda x: x.left)

        activity_df[i] = temp[i]
        activity_df = activity_df.fillna(0)
        weekly_comm_df[i] = activity_df[i]
    return weekly_comm_df


def time_bucket_comm(username, users_df, comm_df, interim_data_path, period):
    today = dt.date.today()
    date_created = users_df.loc[username, 'date_created']
    if period == 'day':
        date_indices = pd.date_range(date_created, today + dt.timedelta(7), freq='D')
    elif period == 'week':
        date_indices = pd.date_range(date_created - dt.timedelta(date_created.weekday()),
                                     today + dt.timedelta(7),
                                     freq='W-MON')
    # TODO: Assertion or something since this is user input?
    activity_columns = comm_activity_columns()
    comm_activity_df = pd.DataFrame(np.nan, index=date_indices, columns=activity_columns)
    if len(comm_df) == 0:
        return comm_activity_df
    thresholds = {}
    for i in ['unrated', 'risky', 'supportive']:
        thresholds[i] = users_df.loc[username, i + '_threshold']

    for i in ['sms_sent', 'sms_received', 'phone_inbound', 'phone_outbound']:
        for j in ['risky', 'neutral', 'supportive', 'unrated']:
            if j == 'risky':
                data = comm_df.loc[((comm_df['relationship'] == 'risky')
                                   | ((comm_df['risk_score'] <= thresholds['risky'])
                                   & (comm_df['risk_score'] > thresholds['unrated'])))
                                   & (comm_df['direction'] == i)]
            elif j == 'neutral':
                data = comm_df.loc[(comm_df['relationship'] != 'risky')
                                   & (comm_df['risk_score'] < thresholds['supportive'])
                                   & (comm_df['risk_score'] > thresholds['risky'])
                                   & (comm_df['direction'] == i)]
            elif j == 'supportive':
                data = comm_df.loc[(comm_df['relationship'] != 'risky')
                                   & (comm_df['risk_score'] >= thresholds['supportive'])
                                   & (comm_df['direction'] == i)]
            elif j == 'unrated':
                data = comm_df.loc[(comm_df['relationship'] != 'risky')
                                   & (comm_df['risk_score'] < thresholds['unrated'])
                                   & (comm_df['direction'] == i)]

            col_name = i + '_' + j
            if len(data) > 0:  # not sure if this is necessary
                temp = data.groupby(pd.cut(data.index, comm_activity_df.index, right=False)).agg({'contactId': pd.Series.count})
                temp.columns = [col_name]
                temp = temp.reset_index()
                temp.index = temp['index'].apply(lambda x: x.left)
                comm_activity_df[col_name] = temp[col_name]
    comm_activity_df = comm_activity_df.fillna(0)
    comm_activity_df['total_comm'] = comm_activity_df.sum(axis=1)  # Needs to immediately follow

    comm_activity_df = add_volume_by_type(comm_activity_df)

    # TODO: reduce how often the datafile is being over written
    interim_data_file_path = os.path.join(interim_data_path, period + '_comm_log_df_' + username + '.pkl')
    comm_activity_df.to_pickle(interim_data_file_path)
    return comm_activity_df


def contact_time_bucket_comm(username, users_df, contacts_df, comm_df, interim_data_path, period):
    today = dt.date.today()
    date_created = users_df.loc[username, 'date_created']
    if period == 'day':
        date_indices = pd.date_range(date_created, today + dt.timedelta(7), freq='D')
    elif period == 'week':
        date_indices = pd.date_range(date_created - dt.timedelta(date_created.weekday()),
                                     today + dt.timedelta(7),
                                     freq='W-MON')
    # activity_columns = comm_activity_columns()
    comm_activity_df = pd.DataFrame(np.nan, index=date_indices, columns=contacts_df.index)
    if len(comm_df) == 0:
        return comm_activity_df

    #     will want inbound, outbound
    for i in contacts_df.index:
        data = comm_df.loc[(comm_df['contactId'] == i)
                             & (comm_df['direction'] != 'phone_finished')]
        col_name = i
        temp = data.groupby(pd.cut(data.index, comm_activity_df.index, right=False)).agg({'contactId': pd.Series.count})
        temp.columns = [col_name]
        temp = temp.reset_index()
        temp.index = temp['index'].apply(lambda x: x.left)
        #             print(temp)
        comm_activity_df[col_name] = temp[col_name]

        data = comm_df.loc[(comm_df['contactId'] == i)
                            & ((comm_df['direction'] == 'sms_received')
                            | (comm_df['direction'] == 'phone_inbound'))]
        col_name = 'inbound_' + i
        temp = data.groupby(pd.cut(data.index, comm_activity_df.index, right=False)).agg({'contactId': pd.Series.count})
        temp.columns = [col_name]
        temp = temp.reset_index()
        temp.index = temp['index'].apply(lambda x: x.left)
        #             print(temp)
        comm_activity_df[col_name] = temp[col_name]

        col_name = 'outbound_' + i
        data = comm_df.loc[(comm_df['contactId'] == i)
                            & ((comm_df['direction'] == 'sms_sent')
                            | (comm_df['direction'] == 'phone_outbound'))]
        temp = data.groupby(pd.cut(data.index, comm_activity_df.index, right=False)).agg({'contactId': pd.Series.count})
        temp.columns = [col_name]
        temp = temp.reset_index()
        temp.index = temp['index'].apply(lambda x: x.left)
        #             print(temp)
        comm_activity_df[col_name] = temp[col_name]


    # thresholds = {}
    # for i in ['unrated', 'risky', 'supportive']:
    #     thresholds[i] = users_df.loc[username, i + '_threshold']
    #
    # for i in ['sms_sent', 'sms_received', 'phone_inbound', 'phone_outbound']:
    #     for j in ['risky', 'neutral', 'supportive', 'unrated']:
    #         if j == 'risky':
    #             data = comm_df.loc[((comm_df['relationship'] == 'risky')
    #                                 | ((comm_df['risk_score'] <= thresholds['risky'])
    #                                    & (comm_df['risk_score'] > thresholds['unrated'])))
    #                                & (comm_df['direction'] == i)]
    #         elif j == 'neutral':
    #             data = comm_df.loc[(comm_df['relationship'] != 'risky')
    #                                & (comm_df['risk_score'] < thresholds['supportive'])
    #                                & (comm_df['risk_score'] > thresholds['risky'])
    #                                & (comm_df['direction'] == i)]
    #         elif j == 'supportive':
    #             data = comm_df.loc[(comm_df['relationship'] != 'risky')
    #                                & (comm_df['risk_score'] >= thresholds['supportive'])
    #                                & (comm_df['direction'] == i)]
    #         elif j == 'unrated':
    #             data = comm_df.loc[(comm_df['relationship'] != 'risky')
    #                                & (comm_df['risk_score'] < thresholds['unrated'])
    #                                & (comm_df['direction'] == i)]
    #
    #         col_name = i + '_' + j
    #         if len(data) > 0:  # not sure if this is necessary
    #             temp = data.groupby(pd.cut(data.index, comm_activity_df.index, right=False)).agg(
    #                 {'contactId': pd.Series.count})
    #             temp.columns = [col_name]
    #             temp = temp.reset_index()
    #             temp.index = temp['index'].apply(lambda x: x.left)
    #             comm_activity_df[col_name] = temp[col_name]
    comm_activity_df = comm_activity_df.fillna(0)
    comm_activity_df['total_comm'] = comm_activity_df.sum(axis=1)  # Needs to immediately follow

    # comm_activity_df = add_volume_by_type(comm_activity_df)

    # TODO: reduce how often the datafile is being over written
    interim_data_file_path = os.path.join(interim_data_path, period + '_contact_comm_log_df_' + username + '.pkl')
    comm_activity_df.to_pickle(interim_data_file_path)
    return comm_activity_df


def comm_df_setup(username, users_df, contacts_df, raw_data_path, interim_data_path):
    comm_df = create_interim_comm_data(username, users_df, contacts_df, raw_data_path, interim_data_path)
    daily_comm_df = time_bucket_comm(username, users_df, comm_df, interim_data_path, 'day')
    weekly_comm_df = time_bucket_comm(username, users_df, comm_df, interim_data_path, 'week')
    if len(comm_df) == 0:
        return

    # These functions don't write to files
    weekly_comm_df = utils.add_weekly_highest_day(daily_comm_df, weekly_comm_df, ['risky_comm', 'total_comm'])
    weekly_comm_df = add_days_with_comm_by_type(daily_comm_df, weekly_comm_df)
    weekly_comm_df = add_percent_comm_for_each_type(weekly_comm_df)
    weekly_comm_df = add_percent_change_in_risky_interactions(weekly_comm_df)
    weekly_comm_df = utils.add_change_values(weekly_comm_df, ['risky_comm_days'])

    # Write to file
    interim_data_file_path = os.path.join(interim_data_path, 'week_comm_log_df_' + username + '.pkl')
    weekly_comm_df.to_pickle(interim_data_file_path)
    # print(weekly_comm_df.columns)
