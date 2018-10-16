import pandas as pd
import datetime as dt
import numpy as np


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


def generate_daily_comm(users_df, data_file_path):  
    users = users_df
    daily_comm_df = pd.DataFrame(np.nan, index=users.index, columns=['total_comm'])
    for e in users:
        activity_columns = comm_activity_columns()
        today = dt.date.today()

        date_indicies = pd.date_range(users[e]['summary_stats']['date_created'], today, freq = 'D')
        activity_df = pd.DataFrame(np.nan, index=date_indicies, columns=activity_columns)
        
        unrated_threshold = users[e]['risk_thresholds']['unrated'].values[0]
        risky_threshold = users[e]['risk_thresholds']['risky'].values[0]
        supportive_threshold = users[e]['risk_thresholds']['supportive'].values[0]
        
        for i in ['sms_sent', 'sms_received', 'phone_inbound', 'phone_outbound']:
            for j in ['risky', 'neutral', 'supportive', 'unrated']:
                if j == 'risky':
                    data = users[e]['comm_activity'].loc[(users[e]['comm_activity']['risk_score'] <= risky_threshold)
                                                         & (users[e]['comm_activity']['risk_score'] > unrated_threshold)
                                                         & (users[e]['comm_activity']['direction'] == i)]
                elif j == 'neutral':
                    data = users[e]['comm_activity'].loc[(users[e]['comm_activity']['risk_score'] < supportive_threshold)
                                                         & (users[e]['comm_activity']['risk_score'] > risky_threshold)
                                                         & (users[e]['comm_activity']['direction'] == i)]
                elif j == 'supportive':
                    data = users[e]['comm_activity'].loc[(users[e]['comm_activity']['risk_score'] >= supportive_threshold)
                                                         & (users[e]['comm_activity']['direction'] == i)]
                elif j == 'unrated':
                    data = users[e]['comm_activity'].loc[(users[e]['comm_activity']['risk_score'] < unrated_threshold)
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