import datetime as dt
import pandas as pd

import make_dataset


def generate_date_indices():
	today = dt.date.today()
	# if today.weekday() == 0:
	# 	last_monday = today - dt.timedelta(7)
	# else:
	# 	last_monday = today - dt.timedelta(today.weekday())
	#
	# start = last_monday - dt.timedelta(21)
	# end = last_monday + dt.timedelta(7)
	# original_method = pd.date_range(start, end, freq='7D')
	new_method = pd.date_range(today-dt.timedelta(28), today + dt.timedelta(0), freq='W-MON')
	# print('time indices comparison')
	# print(original_method)
	print(new_method)
	return new_method


if __name__ == '__main__':

	usernames = ['liamkl', 'vinoct18', 'vinoct24', 'emily', 'zombeck']
	# can also call database_query.get_desired_usernames
	date_indices = generate_date_indices()
	# print('max date')
	# print(max(date_indices).date())
	make_dataset.refresh_user_data(usernames, max(date_indices).date())
