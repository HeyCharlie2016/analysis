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
    comm_activity_df['total_comm'] = comm_activity_df.sum(axis=1)
    # print(comm_activity_df.head())
    interim_data_file_path = os.path.join(interim_data_path, period + '_comm_log_df_' + username + '.pkl')
    comm_activity_df.to_pickle(interim_data_file_path)
