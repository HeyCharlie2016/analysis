import os

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd


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

		#     here = os.path.dirname(__file__)
		#     here = os.path.dirname(os.getcwd())
		here = r'C:\Users\VinnyValant\Documents\Python Stuff\HeyCharlie Reports'
		subdir = 'temp_materials'
		filename = 'WklyCommDays-' + username + '.png'
		filepath = os.path.join(report_chart_path, filename)
		fig.savefig(filepath, bbox_inches="tight")