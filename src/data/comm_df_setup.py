import pandas as pd
import datetime as dt
import numpy as np
import os


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


def sort_daily_comm(users_df, comm_df, data_file_path):
    users = users_df
    # print (users)
    # print(users_df.index)
    # print(users_df.columns)
    daily_comm_df = pd.DataFrame(np.nan, index=users.index, columns=['total_comm'])
    for e in users.index:
        # print(users[e])
        activity_columns = comm_activity_columns()
        today = dt.date.today()
        date_indices = pd.date_range(users.loc[e, 'date_created'], today, freq='D')
        activity_df = pd.DataFrame(np.nan, index=date_indices, columns=activity_columns)

        thresholds = {}
        for i in ['unrated', 'risky', 'supportive']:
            thresholds[i] = users.loc[e, i + '_threshold']

        # unrated_threshold = users[e]['risk_thresholds']['unrated'].values[0]
        # risky_threshold = users[e]['risk_thresholds']['risky'].values[0]
        # supportive_threshold = users[e]['risk_thresholds']['supportive'].values[0]
        
        for i in ['sms_sent', 'sms_received', 'phone_inbound', 'phone_outbound']:
            for j in ['risky', 'neutral', 'supportive', 'unrated']:
                if j == 'risky':
                    data = users.loc[e, 'comm_activity'].loc[(users[e]['comm_activity']['risk_score'] <= thresholds['risky'])
                                                         & (users[e]['comm_activity']['risk_score'] > thresholds['unrated'])
                                                         & (users[e]['comm_activity']['direction'] == i)]
                elif j == 'neutral':
                    data = users.loc[e, 'comm_activity'].loc[(users[e]['comm_activity']['risk_score'] < thresholds['supportive'])
                                                         & (users[e]['comm_activity']['risk_score'] > thresholds['risky'])
                                                         & (users[e]['comm_activity']['direction'] == i)]
                elif j == 'supportive':
                    data = users.loc[e, 'comm_activity'].loc[(users[e]['comm_activity']['risk_score'] >= thresholds['supportive'])
                                                         & (users[e]['comm_activity']['direction'] == i)]
                elif j == 'unrated':
                    data = users.loc[e, 'comm_activity'].loc[(users[e]['comm_activity']['risk_score'] < thresholds['unrated'])
                                                         & (users[e]['comm_activity']['direction'] == i)]
                
                col_name = i + '_' + j
                temp = data.groupby(pd.cut(data.index, activity_df.index, right=False)).agg({'contactId': pd.Series.count})
                temp.columns = [col_name]
                temp = temp.reset_index()
                temp.index = temp['index'].apply(lambda x: x.left)

                activity_df[col_name] = temp[col_name]

        activity_df = activity_df.fillna(0)
        activity_df['total_comm'] = activity_df.sum(axis=1)

        daily_comm_df[e] = activity_df
        daily_comm_df.to_pickle(data_file_path)