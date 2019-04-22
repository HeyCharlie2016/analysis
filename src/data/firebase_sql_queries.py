from google.cloud import bigquery

import pathlib # __file__ isn't recognized in Jupyter, so we need this to get the root path


def init_sql_connection():
	path = pathlib.Path.cwd()
	PROJ_ROOT = path.parent

	accountkeyfile = str(PROJ_ROOT.parent / 'credentials\GoogleServiceAccountKeyFile.json')
	return bigquery.Client.from_service_account_json(accountkeyfile)


def get_day_text_events(client, username):
	query = ("""
	SELECT
	    event_date AS Date,
	    SUM(CASE WHEN event_name = "text_message_received" THEN 1 ELSE 0 END) AS in_sms,
    	SUM(CASE WHEN event_name = "text_message_sent" THEN 1 ELSE 0 END) AS sent_sms,
	    SUM(CASE WHEN event_name = "risky_sent_sms_warning_event" THEN 1 ELSE 0 END) AS risky_sent_sms,
	    SUM(CASE WHEN event_name = "risky_sent_sms_warning_notification" THEN 1 ELSE 0 END) AS risky_sent_notification,
	    SUM(CASE WHEN event_name = "risky_sms_event" THEN 1 ELSE 0 END) AS risky_in_sms,
	    SUM(CASE WHEN event_name = "risky_sms_warning_notification" THEN 1 ELSE 0 END) AS risky_in_notification,
	    COUNT(event_name) AS events
	FROM 
	    `analytics_153084895.events_*`,
		UNNEST(user_properties) AS user_properties
	WHERE
		user_properties.value.string_value = '""" + username + """'
	    AND(event_name = "risky_sent_sms_warning_event"
	    OR event_name = "risky_sent_sms_warning_notification"
	    OR event_name = "risky_sms_event"
	    OR event_name = "risky_sms_warning_notification"
	    OR event_name = 'text_message_received'
        OR event_name = 'text_message_sent')
	GROUP BY
	    event_date
	ORDER BY
	    event_date DESC
	""")
	df = client.query(query).to_dataframe()
	return df


def get_text_events(client, username):
	query = ("""
	SELECT
	    event_timestamp AS Timestamp,
	    SUM(CASE WHEN event_name = "text_message_received" THEN 1 ELSE 0 END) AS in_sms,
    	SUM(CASE WHEN event_name = "text_message_sent" THEN 1 ELSE 0 END) AS sent_sms,
	    SUM(CASE WHEN event_name = "risky_sent_sms_warning_event" THEN 1 ELSE 0 END) AS risky_sent_sms,
	    SUM(CASE WHEN event_name = "risky_sent_sms_warning_notification" THEN 1 ELSE 0 END) AS risky_sent_notification,
	    SUM(CASE WHEN event_name = "risky_sms_event" THEN 1 ELSE 0 END) AS risky_in_sms,
	    SUM(CASE WHEN event_name = "risky_sms_warning_notification" THEN 1 ELSE 0 END) AS risky_in_notification,
	    COUNT(event_name) AS events
	FROM 
	    `analytics_153084895.events_*`,
		UNNEST(user_properties) AS user_properties
	WHERE
		user_properties.value.string_value = '""" + username + """'
	    AND(event_name = "risky_sent_sms_warning_event"
	    OR event_name = "risky_sent_sms_warning_notification"
	    OR event_name = "risky_sms_event"
	    OR event_name = "risky_sms_warning_notification")
	GROUP BY
	    event_timestamp
	ORDER BY
	    event_timestamp DESC
	""")
	df = client.query(query).to_dataframe()
	return df


def get_reviver_events(client, username):
	query = ('''
	    SELECT
	      event_timestamp AS timestamp,
	      event_name AS event
	    FROM
	      `analytics_153084895.events_*`,
	      UNNEST(user_properties) AS user_properties
	    WHERE 
	      (event_name = 'reviver_scimitar_already_running'
	      OR event_name = 'reviver_scimitar_starting')
	      AND user_properties.value.string_value = "''' + username + '''"
	    ORDER BY
	      event_timestamp DESC''')
	df = client.query(query).to_dataframe()
	return df