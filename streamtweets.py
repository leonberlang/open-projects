# -*- coding: utf-8 -*-

# Tweepy Twitter API access
from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler 
from tweepy import Stream

# Data storing
from datetime import datetime
import time
import re 
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Other Python files for queries
import crd
import antivax

class StdOutListener(StreamListener):

	def on_data(self, data):
		try:
			# Load in full json file into one variable (accessible as python dictionary)
			tweet = json.loads(data)
			tweet_row = []

			# Tweet metadata
			tweet_id = tweet_row.append(tweet["id_str"])
			timestamp = tweet_row.append(tweet["created_at"])
			is_retweet = tweet_row.append(tweet["retweeted"])

			# Tweet CONTENT cleaning and analysis
			if tweet["truncated"] == True:
				tweet_row.append(re.sub('[^0-9a-zA-Z#.@_:/—-]+', ' ', tweet["extended_tweet"]["full_text"]))
			elif "retweeted_status" in tweet:
				if "extended_tweet" in tweet["retweeted_status"]:
					RT = tweet_row.append(re.sub('[^0-9a-zA-Z#.@_:/—-]+', ' ', tweet["retweeted_status"]["extended_tweet"]["full_text"]))
					tweet_row[2] = True
				elif "text" in tweet["retweeted_status"]:
					RT = tweet_row.append(re.sub('[^0-9a-zA-Z#.@_:/—-]+', ' ', tweet["retweeted_status"]["text"]))
					tweet_row[2] = True	
			elif 'text' in tweet:
				TWT = tweet_row.append(re.sub('[^0-9a-zA-Z#.@_:/—-]+', ' ', tweet["text"]))

			geo = tweet["place"]
			if geo is None:
				geo = "none"
			tweet_row.append(geo)

			hashtag_list = tweet["entities"]["hashtags"]
			hashtags = []
			for hashtag in hashtag_list:
				hashtags.append(hashtag["text"])
			tweet_row.append(','.join(hashtags))
			retweets = tweet_row.append(tweet["retweet_count"])
			favorites = tweet_row.append(tweet["favorite_count"])
			source = tweet_row.append(tweet["source"])

			# Relevant user data
			user_id = tweet_row.append(tweet["user"]["id_str"])
			username = tweet_row.append(tweet["user"]["name"])
			screen_name = tweet_row.append(tweet["user"]["screen_name"])
			language = tweet_row.append(tweet["user"]["lang"])
			location = tweet_row.append(tweet["user"]["location"])	

			# Other user data
			description = tweet_row.append(tweet["user"]["description"])
			account_date = tweet_row.append(tweet["user"]["created_at"])

			# Logging timestamp
			ct = tweet_row.append(str(datetime.now()))
			
			# User feedback
			print(tweet_row)
			
			# Setting up Google Sheets connection and dumping tweet into sheet
			scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
			credentials = ServiceAccountCredentials.from_json_keyfile_name('GRADUATION-DDD-LEON-800a49a0a94a.json', scope)
			gc = gspread.authorize(credentials)
			wks = gc.open('antivax-tweets').sheet1
	
			wks.append_row(tweet_row)
			
			return True

		except BaseException as e:
			print("Something went wrong...: %s" % str(e))
			return True
	
	def on_error(self, status):
		print(status)	

	def on_error(self, status_code):
		if status_code == 420:
			print("rate limit has been blazed... we waitin an hour")
		time.sleep(3601)
		return True		

if __name__ == "__main__":
	word_list = antivax.TERMS
	allowed_lang = antivax.LANGUAGES
	listener = StdOutListener()
	auth = OAuthHandler(crd.CONSUMER_KEY, crd.CONSUMER_SECRET)
	auth.set_access_token(crd.ACCESS_TOKEN, crd.ACCESS_TOKEN_SECRET)
	stream = Stream(auth, listener, tweet_mode= 'extended',wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
	stream.filter(languages=allowed_lang, track=word_list)		