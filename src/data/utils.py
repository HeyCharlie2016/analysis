import pandas as pd
import numpy as np
import datetime as dt

def add_weekly_highest_day(daily_df, weekly_df, labels):
	# i = 'total_comm'
	weekday_dict = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
	data = daily_df[labels]
	# date_indicies = pd.date_range(start, end, freq='7D')
	date_indices = weekly_df.index

	activity_df = pd.DataFrame(np.nan, index=date_indices, columns=labels)

	# makes columns of the corresponding weekly max next to each index
	temp = data.groupby(pd.cut(data.index, activity_df.index, right=False)).transform(max)
	for i in labels:
		temp['max_' + i] = temp[i] == data[i]
		for j in weekly_df.index:
			if weekly_df.loc[j, i] == 0:
				activity_df.loc[j, i] = '- No Max -'
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
		weekly_df['high_' + i + '_day'] = activity_df[i]
	return weekly_df


def add_change_values(df, cols):
	for col in cols:
		current_values = df[col][1:]
		prev_values = df[col].shift(1)
		df['change_in_' + col] = (current_values - prev_values)
	return df
