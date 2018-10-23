import datetime as dt
import pandas as pd

import make_dataset


def generate_date_indices():
	today = dt.date.today()
	if today.weekday() == 0:
		last_monday = today - dt.timedelta(7)
	else:
		last_monday = today - dt.timedelta(today.weekday())

	start = last_monday - dt.timedelta(21)
	end = last_monday + dt.timedelta(7)

	return pd.date_range(start, end, freq='7D')


if __name__ == '__main__':

	usernames = ['liamkl', 'vinoct18', 'vinvin', 'emily', 'zombeck']
	# can also call database_query.get_desired_usernames
	date_indicies = generate_date_indices()

	make_dataset.refresh_user_data(usernames)
