import os

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.image as mpimg
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

		filename = 'days_w_comm_chart' + '-' + username + '.png'
		filepath = os.path.join(report_chart_path, filename)
		fig.savefig(filepath, bbox_inches="tight")


def comm_vol_bar_chart(usernames, date_indices, comm_vol_bar_chart_data, report_chart_path):
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

	for count, username in enumerate(usernames):
		fig = plt.figure()
		ax = fig.add_subplot(111)
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

		filename = 'weekly_comm_chart' + '-' + username + '.png'
		filepath = os.path.join(report_chart_path, filename)
		fig.savefig(filepath, bbox_inches="tight")

		# img = mpimg.imread(filepath)
		# imgplot = plt.imshow(img)
		# plt.show()


def comm_pie_chart(usernames, date_index, comm_pie_chart_data, report_chart_path):
	for count, username in enumerate(usernames):
		fig = plt.figure()
		ax = fig.add_subplot(111)

		chart_colors = {'risky': '#e65c00',
						'neutral': '#b3b3ff',
						'unrated': '#C0C0C0',
						'supportive': '#009900'}
		labels = ['Risky', 'Neutral', 'Supportive', 'Unrated']

		cols = ['risky_percent', 'neutral_percent', 'supportive_percent', 'unrated_percent']
		labels = ['Risky', 'Neutral', 'Supportive', 'Unrated']
		explode = [0, 0, 0, 0]

		colors = []
		for k in labels:
			colors.append(chart_colors[k.lower()])
		# print(date_index)
		# data = comm_vol_bar_chart_data.xs(username)[min(date_indices):max(date_indices)]
		data = comm_pie_chart_data.xs(username)
		# print(data.index)
		# print(data)

		if not np.isnan(data[cols[0]]):
			# data = comm_pie_chart_data.loc[username]

			#         ORIGINAL - no data labels
			#         chart = ax.pie( users[e]['weekly_activity'][cols].loc[users[e]['weekly_activity'].index[-2]],
			#                      explode=explode, colors = colors)
			#         chart = ax.pie(data, explode=explode, colors = colors)

			#         First Attempt - dummy data labels
			#         def func(pct, allvals):
			#             absolute = int(pct/100.*np.sum(allvals))
			#             return "\n{:.1f}%".format(pct, absolute)
			#         wedges, texts, autotexts = ax.pie(data, autopct=lambda pct: func(pct, data), textprops=dict(color="w"))

			#         Second Attempt - fancy data lables
			wedges, texts = ax.pie(data, explode=explode, colors=colors)
			kw = dict(xycoords='data', textcoords='data', zorder=0, va="center",
					  arrowprops=dict(facecolor='silver', arrowstyle="-"))
			low_count = 0
			for i, p in enumerate(wedges):
				if data[i] > 0.02:
					ang = (p.theta2 - p.theta1) / 2. + p.theta1
					y = np.sin(np.deg2rad(ang))
					x = np.cos(np.deg2rad(ang))
					horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
					connectionstyle = "angle,angleA=0,angleB={}".format(ang)
					kw["arrowprops"].update({"connectionstyle": connectionstyle})

					text = labels[i] + '\n' + '{:.0%}'.format(data[i])
					text_x = x + (1 - x) * 2 / 3 * np.sign(
						x)  # setting the x coordinate of the text-box to be halfway between the cahrt and x = 1 (or -1)
					if abs(text_x) > 1:
						text_x = np.sign(x)
					ax.annotate(text, xy=(x, y), xytext=(text_x, 1.2 * y),
								horizontalalignment=horizontalalignment, **kw, fontsize=14)
				else:
					text = labels[i] + ': ' + '{:.0%}'.format(data[i])
					ax.annotate(text, xy=(1.5, 0.2 * low_count - 1), fontsize=14, ha='center')
					low_count += 1
			ax.axis('equal')
		else:
			ax.annotate("No Communication last week", xy=(0.5, 0.5), fontsize=14, va='center',
						ha='center')
			ax.spines['left'].set_visible(False)
			ax.spines['right'].set_visible(False)
			ax.spines['top'].set_visible(False)
			ax.spines['bottom'].set_visible(False)
			plt.tick_params(
				#             axis='x',          # changes apply to the x-axis
				which='both',  # both major and minor ticks are affected
				bottom=False,  # ticks along the bottom edge are off
				top=False,  # ticks along the top edge are off
				labelbottom=False,
				right=False,
				left=False,
				labelleft=False)

		filename = 'comm_pie_chart' + '-' + username + '.png'
		filepath = os.path.join(report_chart_path, filename)
		fig.savefig(filepath, bbox_inches="tight")