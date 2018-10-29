import os

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import math


def dates_to_strings(date_indices):
	date_strings = []
	for time in date_indices:
		date_strings.append(time.strftime('%b %d'))
	return date_strings


def comm_days_line_chart(usernames, date_indices, comm_days_line_chart_data, report_chart_path):
	for count, username in enumerate(usernames):
		fig = plt.figure()
		ax = fig.add_subplot(111)

		date_indicies = date_indices.tolist()
		colors = ['#C0C0C0', '#00cc00', '#ff6600']
		cols = ['total_comm_days', 'risky_comm_days', 'supportive_comm_days']
		labels = ['Any Comm', 'Risky Comm', 'Supportive Comm']
		linestyle = ['solid', 'dashed', 'dotted']

		data = comm_days_line_chart_data.xs(username)
		for i, c in enumerate(data[cols][:-1]):
			ax.plot(data[c][:-1], label=labels[i], linewidth=5.0,
					marker='o', linestyle=linestyle[i])
			for k, j in enumerate(data[c][:-1]):
				if j > 0:
					ax.annotate(int(j), xy=(data[c][:-1].index[k],
											j + max(0.2, j / 15)), fontsize=14, va='bottom',
								ha='center')
		plt.xticks(date_indicies[:-1], fontsize=14, rotation='30')
		ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))

		top = 7.9
		ax.set_ylim(bottom=0, top=top)
		plt.yticks(np.arange(0, top, 2), fontsize=14)
		plt.xlabel('Week of:', fontsize=14)
		plt.ylabel('Days with Interactions', fontsize=14)
		ax.legend(bbox_to_anchor=(1, 1), loc=2, frameon=False, fontsize=14)
		ax.spines['right'].set_visible(False)
		ax.spines['top'].set_visible(False)

		filename = 'WklyCommDays-' + username + '.png'
		filepath = os.path.join(report_chart_path, filename)
		fig.savefig(filepath, bbox_inches="tight")


def comm_vol_bar_chart(usernames, date_indices, comm_vol_bar_chart_data, report_chart_path):
	for count, username in enumerate(usernames):
		fig = plt.figure()
		ax = fig.add_subplot(111)

		chart_colors = {'risky': '#e65c00',
						'neutral': '#b3b3ff',
						'unrated': '#C0C0C0',
						'supportive': '#009900'}
		labels = ['Risky', 'Neutral', 'Supportive', 'Unrated']
		cols = ['risky_comm', 'neutral_comm', 'supportive_comm', 'unrated_comm']
		date_strings = dates_to_strings(date_indices)

		colors = []
		for k in labels:
			colors.append(chart_colors[k.lower()])

		data = comm_vol_bar_chart_data.xs(username)[min(date_indices):max(date_indices)]
		rolling_total = pd.DataFrame(0, data[:-1].index, columns=['temp'])

		for i, col in enumerate(data[cols]):
			if i == 0:
				ax.bar(date_strings, data[col], 0.7, color=colors[i], label=labels[i])
				rolling_total = data[col]
			else:
				ax.bar(date_strings, data[col], 0.7, color=colors[i], label=labels[i], bottom=rolling_total)
				rolling_total = rolling_total + data[col]
				if i == len(cols) - 1:
					for k, j in enumerate(rolling_total):
						if j > 0:
							ax.annotate(int(j), xy=(date_strings[k], j), fontsize=14, va='bottom', ha='center')

		plt.xticks(date_strings, fontsize=14, rotation='30')
		top = max(rolling_total.max() * 1.2, rolling_total.max() + 2.1, 5)
		ax.set_ylim(bottom=0, top=top)
		interval = 1
		for j in [2, 5, 10, 15, 20, 25, 50, 100, 200, 250, 500, 1000]:
			if math.ceil(top / 4) > j:
				interval = j
			else:
				break
		plt.yticks(np.arange(0, top, interval), fontsize=14)
		plt.xlabel('Week of:', fontsize=14)
		plt.ylabel('Number of Interactions', fontsize=14)
		ax.spines['right'].set_visible(False)
		ax.spines['top'].set_visible(False)

		handles, labels = ax.get_legend_handles_labels()
		ax.legend(handles[::-1], labels[::-1], loc=2, bbox_to_anchor=(1, 1), frameon=False, fontsize=14)

		filename = 'WklyComm-' + username + '.png'
		filepath = os.path.join(report_chart_path, filename)
		fig.savefig(filepath, bbox_inches="tight")