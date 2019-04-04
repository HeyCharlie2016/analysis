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


def comm_days_line_chart(usernames, date_indices, comm_days_line_chart_data, report_chart_path, **kwargs):
	show = kwargs.get('show', False)
	for count, username in enumerate(usernames):
		fig = plt.figure()
		ax = fig.add_subplot(111)

		colors = ['#C0C0C0', '#00cc00', '#ff6600']
		cols = ['total_comm_days', 'risky_comm_days', 'supportive_comm_days']
		labels = ['Any Comm', 'Risky Comm', 'Supportive Comm']
		linestyle = ['solid', 'dashed', 'dotted']
		if isinstance(comm_days_line_chart_data.index, pd.core.index.MultiIndex):
			data = comm_days_line_chart_data.xs(username)
		else:
			data = comm_days_line_chart_data

		date_range = list(set(date_indices) & set(data.index))
		# date_range = date_range.tolist()

		for i, c in enumerate(data[cols][:-1]):
			ax.plot(data[c][:-1], label=labels[i], linewidth=5.0,
					marker='o', linestyle=linestyle[i])
			for k, j in enumerate(data[c][:-1]):
				if j > 0:
					ax.annotate(int(j), xy=(data[c][:-1].index[k],
								j + max(0.2, j / 15)), fontsize=14, va='bottom',
								ha='center')
		plt.xticks(date_range, fontsize=14, rotation='30')
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
		# plt.clf()
		if show:
			plt.show()
		plt.close(fig)


def comm_vol_bar_chart(usernames, date_indices, comm_vol_bar_chart_data, report_chart_path, **kwargs):
	show = kwargs.get('show', False)
	chart_colors = {'risky': '#e65c00',
					'neutral': '#b3b3ff',
					'unrated': '#C0C0C0',
					'supportive': '#009900'}
	labels = ['Risky', 'Neutral', 'Supportive', 'Unrated']
	cols = ['risky_comm', 'neutral_comm', 'supportive_comm', 'unrated_comm']
	# date_strings = dates_to_strings(date_indices)
	colors = []
	for k in labels:
		colors.append(chart_colors[k.lower()])

	for count, username in enumerate(usernames):
		fig = plt.figure()
		ax = fig.add_subplot(111)
		if isinstance(comm_vol_bar_chart_data.index, pd.core.index.MultiIndex):
			data = comm_vol_bar_chart_data.xs(username)[min(date_indices):max(date_indices)]
		else:
			data = comm_vol_bar_chart_data[cols]

		date_range = sorted(list(set(date_indices) & set(data.index)))
		date_strings = dates_to_strings(date_range)
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
		# plt.clf()
		if show:
			plt.show()
		plt.close(fig)


def comm_pie_chart(usernames, date_index, comm_pie_chart_data, report_chart_path, **kwargs):
	show = kwargs.get('show', False)
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
		# if len(comm_pie_chart_data.index) > 1:
		if username in comm_pie_chart_data.index:
			data = comm_pie_chart_data.loc[username]
		else:
			data = comm_pie_chart_data

		if not np.isnan(data[cols[0]]):
			if sum(data) > 0:
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
						text_x = x + (1 - x) * 2 / 3 * np.sign(x)
						# setting the x coordinate of the text-box to be halfway between the cahrt and x = 1 (or -1)
						if abs(text_x) > 1:
							text_x = np.sign(x)
						ax.annotate(text, xy=(x, y), xytext=(text_x, 1.2 * y),  #
									horizontalalignment=horizontalalignment, fontsize=14)  # **kw causes an error for 100%
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
		# fig.savefig(filepath)
		# plt.clf()
		if show:
			plt.show()
		plt.close(fig)


def loc_days_bar_chart(usernames, date_indices, loc_days_bar_chart_data, report_chart_path, **kwargs):
	show = kwargs.get('show', False)
	chart_colors = {'risky': '#e65c00',
					'neutral': '#b3b3ff',
					'unrated': '#C0C0C0',
					'supportive': '#009900'}
	label = 'Days with Risky Location Visits'
	col = 'days_w_risky_loc_visits'
	# date_strings = dates_to_strings(date_indices)
	color = '#e65c00'

	for count, username in enumerate(usernames):
		fig = plt.figure()
		ax = fig.add_subplot(111)
		# data = loc_days_bar_chart_data.xs(username)[min(date_indices):max(date_indices)]
		if isinstance(loc_days_bar_chart_data.index, pd.core.index.MultiIndex):
			data = loc_days_bar_chart_data.xs(username)[min(date_indices):max(date_indices)]
		else:
			data = loc_days_bar_chart_data

		date_range = sorted(list(set(date_indices) & set(data.index)))
		date_strings = dates_to_strings(date_range)

		ax.bar(date_strings, data[col], 0.7, color=color, label=label)
		for k, j in enumerate(data[col]):
			if j > 0:
				ax.annotate(int(j), xy=(date_strings[k], j), fontsize=14, va='bottom', ha='center')

		top = 7.9
		ax.set_ylim(bottom=0, top=top)
		plt.yticks(np.arange(0, top, 2), fontsize=14)
		plt.xlabel('Week of:', fontsize=14)
		plt.ylabel(label, fontsize=14)
		if sum(data[col]) == 0:
			# print(date_strings)
			ax.annotate("No Risky Location Visits", xy=(date_strings[0], top / 2), fontsize=14, va='center', ha='left')

		plt.xticks(date_strings, fontsize=14, rotation='30')
		ax.spines['right'].set_visible(False)
		ax.spines['top'].set_visible(False)

		# handles, labels = ax.get_legend_handles_labels()
		# ax.legend(handles[::-1], labels[::-1], loc=2, bbox_to_anchor=(1, 1), frameon=False, fontsize=14)

		filename = 'days_w_risky_loc_chart' + '-' + username + '.png'
		filepath = os.path.join(report_chart_path, filename)
		fig.savefig(filepath, bbox_inches="tight")
		# plt.clf()
		if show:
			plt.show()
		plt.close(fig)
