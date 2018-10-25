import pandas as pd
import datetime as dt
import numpy as np
import os


def comm_activity_columns():
    activity_columns = []
    for i in ['sms_sent', 'sms_received', 'phone_inbound', 'phone_outbound']:
        for j in ['risky', 'neutral', 'supportive', 'unrated']:
            if i == 'locations':
                if j == 'neutral' or j == 'unrated':
                    pass
                else:
                    activity_columns.append(i + '_' + j)
            else:
                activity_columns.append(i + '_' + j)
    return activity_columns



def create_interim_comm_data(username, users_df, contacts_df, raw_data_path, interim_data_path):
    user_id = users_df.loc[username, 'userId']
    raw_data_file_path = os.path.join(raw_data_path, 'comm_log_df_' + user_id + '.pkl')
    raw_comm_df = pd.read_pickle(raw_data_file_path)
    user_activity = raw_comm_df[raw_comm_df['userId'] == user_id]

    risk_scores = pd.Series(np.empty(len(user_activity.index)), index=user_activity.index)
    for index, row in user_activity.iterrows():
        contact_id = row['contactId']
        risk_scores[index] = contacts_df.loc[contact_id]['score']
    user_activity = user_activity.assign(risk_score=risk_scores.values)
    comm_df = user_activity[['contactId', 'direction', 'timestamp', 'risk_score']]
    comm_df.index = pd.to_datetime(comm_df['timestamp'], unit="ms") - dt.timedelta(hours=4)

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


def add_weekly_highest_day(daily_comm_df, weekly_comm_df):
    i = 'total_comm'
    weekday_dict = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
    labels = ['risky_comm', 'total_comm']
    cols = ['high_risky_comm_day', 'high_total_comm_day']

    data = daily_comm_df[labels]
    # date_indicies = pd.date_range(start, end, freq='7D')
    date_indices = weekly_comm_df.index
    activity_df = pd.DataFrame(np.nan, index=date_indices, columns=labels)

    # makes columns of the corresponding weekly max next to each index
    temp = data.groupby(pd.cut(data.index, activity_df.index, right=False)).transform(max)
    for i in labels:
        temp['max_' + i] = temp[i] == data[i]
        for j in weekly_comm_df.index:
            if weekly_comm_df[i][j] == 0:
                activity_df[i][j] = '- No Max -'
            else:
                week_temp = temp[j:j + dt.timedelta(7)]
                max_dates = week_temp[week_temp['max_' + i]].index
                days = []
                for k in max_dates:
                    days.append(weekday_dict[k.weekday()])
                if len(days) < 4:
                    activity_df.loc[j] = ', '.join(days)
                else:
                    activity_df.loc[j] = '- No Max -'
        weekly_comm_df['high_' + i + '_day'] = activity_df[i]
    return weekly_comm_df
    # users['vinsep11']['weekly_activity'][['risky_comm', 'total_comm', 'high_risky_comm_day', 'high_total_comm_day']]

def time_bucket_comm(username, users_df, comm_df, interim_data_path, period):
    today = dt.date.today()
    date_created = users_df.loc[username, 'date_created']
    if period == 'day':
        date_indices = pd.date_range(date_created, today, freq='D')
    elif period == 'week':
        date_indices = pd.date_range(date_created, today, freq='W-MON')
    # TODO: Assertion or something since this is user input?
    activity_columns = comm_activity_columns()
    comm_activity_df = pd.DataFrame(np.nan, index=date_indices, columns=activity_columns)

    thresholds = {}
    for i in ['unrated', 'risky', 'supportive']:
        thresholds[i] = users_df.loc[username, i + '_threshold']

    for i in ['sms_sent', 'sms_received', 'phone_inbound', 'phone_outbound']:
        for j in ['risky', 'neutral', 'supportive', 'unrated']:
            if j == 'risky':
                data = comm_df.loc[(comm_df['risk_score'] <= thresholds['risky'])
                                   & (comm_df['risk_score'] > thresholds['unrated'])
                                   & (comm_df['direction'] == i)]
            elif j == 'neutral':
                data = comm_df.loc[(comm_df['risk_score'] < thresholds['supportive'])
                                   & (comm_df['risk_score'] > thresholds['risky'])
                                   & (comm_df['direction'] == i)]
            elif j == 'supportive':
                data = comm_df.loc[(comm_df['risk_score'] >= thresholds['supportive'])
                                   & (comm_df['direction'] == i)]
            elif j == 'unrated':
                data = comm_df.loc[(comm_df['risk_score'] < thresholds['unrated'])
                                   & (comm_df['direction'] == i)]

            col_name = i + '_' + j
            temp = data.groupby(pd.cut(data.index, comm_activity_df.index, right=False)).agg({'contactId': pd.Series.count})
            temp.columns = [col_name]
            temp = temp.reset_index()
            temp.index = temp['index'].apply(lambda x: x.left)
            comm_activity_df[col_name] = temp[col_name]
    comm_activity_df = comm_activity_df.fillna(0)
    comm_activity_df['total_comm'] = comm_activity_df.sum(axis=1) # Needs to immediately follow

    comm_activity_df = add_volume_by_type(comm_activity_df)

    # print(comm_activity_df.head())
    # TODO: reduce how often the datafile is being over written
    interim_data_file_path = os.path.join(interim_data_path, period + '_comm_log_df_' + username + '.pkl')
    comm_activity_df.to_pickle(interim_data_file_path)


def comm_df_setup(username, users_df, contacts_df, raw_data_path,
                                                         interim_data_path):
    comm_df = create_interim_comm_data(username, users_df, contacts_df, raw_data_path,
                                                     interim_data_path)
    daily_comm_df = time_bucket_comm(username, users_df, comm_df, interim_data_path, 'day')
    weekly_comm_df = time_bucket_comm(username, users_df, comm_df, interim_data_path, 'week')

    # These functions don't write to files
    weekly_comm_df = add_weekly_highest_day(daily_comm_df, weekly_comm_df)
    interim_data_file_path = os.path.join(interim_data_path,'week_comm_log_df_' + username + '.pkl')
    weekly_comm_df.to_pickle(interim_data_file_path)
    print(weekly_comm_df.columns)
