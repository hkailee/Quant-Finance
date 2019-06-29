#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import csv

#http://www.tweepy.org/
import tweepy

#Get your Twitter API credentials and enter them here
consumer_key = "RviM6aIq5tf6pJdh8WIZUwKZS"
consumer_secret = "W53VhhqAGdO6arWUrzJSJ5VKNfdqY7SsAvZq8H2PwRRg4Fr1pE"
access_key = "708296184931090437-0l8LZz3xdvXliAWwpeNSUhBhAvP6D09"
access_secret = "rM7yzKyRSE0kIwmnjnVSSeWoKmjPbJI7aWiFs9iMyEsQ8"

#method to get a user's last tweets
def get_tweets(username):

	#http://tweepy.readthedocs.org/en/v3.1.0/getting_started.html#api
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_key, access_secret)
	api = tweepy.API(auth)

	#set count to however many tweets you want
	number_of_tweets = 100

	#get tweets
	tweets_for_csv = []
	for tweet in tweepy.Cursor(api.user_timeline, screen_name = username, tweet_mode='extended').items(number_of_tweets):
        #create array of tweet information: username, tweet id, date/time, text
		tweets_for_csv.append([username, tweet.id_str, tweet.created_at, tweet.full_text.encode("utf-8")])

	#write to a new csv file from the array of tweets
	outfile = username + "_tweets.csv"
	print "writing to " + outfile
	with open(outfile, 'w+') as file:
		writer = csv.writer(file, delimiter=',')
		writer.writerows(tweets_for_csv)


#if we're running this as a script
if __name__ == '__main__':

    #get tweets for username passed at command line
    if len(sys.argv) == 2:
        get_tweets(sys.argv[1])
    else:
        print "Error: enter one username"

    #alternative method: loop through multiple users
	# users = ['user1','user2']

	# for user in users:
	# 	get_tweets(user)