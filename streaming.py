#!/usr/bin/python3

"""
Filename: followers.py
"""

import sys
import argparse
import json
import time
import math
import tweepy
import couchdb

"""
  Function Definitions
"""

# This function stores a tweet to a CouchDB database
def store_tweet ( tweet, database ):
  # Store the serialisable JSON data in a string (tweet is a 'Status' object)
  tweet_str = json.dumps(tweet._json)
  # Decode JSON string
  tweet_doc = json.loads(tweet_str)
  # Store the unique tweet ID as the document _id for CouchDB
  tweet_doc.update({'_id': tweet.id_str})
  # Attempt to save tweet to CouchDB
  try:
    database.save(tweet_doc)
    print ("Tweet " + tweet.id_str + " stored in database " + str(database.name))
  # A ResourceConflict exception is raised if the _id already exists
  except ResourceConflict:
    print ("Tweet " + tweet.id_str + " already exists in database " + str(database.name))
  # A ResourceNotFound exception is raised if the PUT request returns HTTP 404
  except ResourceNotFound:
    print ("Tweet " + tweet.id_str + "store attempt failed... trying again in 5 seconds...")
    # There's no point continuing iterating if tweets aren't being stored, so try again when CouchDB is back
    time.sleep(5)
    store_tweet( tweet, database )
  # If it's an unknown error, continue for now, but warn the user
  except:
    print ("Unexpected error storing tweet " + tweet.id_str + ": " + str(sys.exc_info()[0]))
    pass

""" --------------------------
    Main Program
-------------------------- """

# Create a log file
#log_file = open("message.log","w")
#sys.stdout = log_file

# Parse command line arguments
parser = argparse.ArgumentParser(description='')
parser.add_argument('--couchip', '-c', required=True, help='The IP address of the CouchDB instance')
parser.add_argument('--consumerkey', '-ck', required=True, help='Twitter API Consumer Key')
parser.add_argument('--consumersecret', '-cs', required=True, help='Twitter API Consumer Secret')
parser.add_argument('--tokenkey', '-tk', required=True, help='Twitter Access Token Key')
parser.add_argument('--tokensecret', '-ts', required=True, help='Twitter Access Token Secret')
args = parser.parse_args()

# Initialise Twitter communication
auth = tweepy.OAuthHandler(args.consumerkey, args.consumersecret)
auth.set_access_token(args.tokenkey, args.tokensecret)
try:
  api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
  # tweepy.API constructor does not seem to throw an exception for OAuth failure
  # Use API.verify_credentials() to validate OAuth instead
  cred = api.verify_credentials()
  print ("OAuth connection with Twitter established through user @" + cred.screen_name + "\n")
except tweepy.TweepError as oauth_error:
  print ("OAuth connection with Twitter could not be established\n\n")
  raise oauth_error
except:
  raise

# Initialise CouchDB communication
db_tweets_str = 'tweets'
try:
  couch = couchdb.Server('http://' + args.couchip + ':5984/')
  print ("Connected to CouchDB server at http://" + args.couchip + ":5984\n")
except:
  print ("Failed to connect to CouchDB server at http://" + args.couchip + ":5984\n\n")
  raise
try:
  db_tweets = couch[db_tweets_str]
  print ("Connected to " + db_tweets_str + " database")
# The python-couchdb documentation says that a PreconditionFailed exception is raised when a DB isn't found
# But in practice it throws a ResourceNotFound exception
except couchdb.ResourceNotFound:
  try:
    db_tweets = couch.create(db_tweets_str)
    print ("Creating new database: " + db_tweets_str)
  except:
    raise
except:
  raise

# Set up Tweepy stream listener
class TweetStreamListener(tweepy.StreamListener):
  
  def on_status(self, status):
    store_tweet(status, db_tweets)

tweetStreamListener = TweetStreamListener()
melbourneStream = tweepy.Stream(auth = api.auth, listener=tweetStreamListener)
melbourneStream.filter(locations=[144.4441,-38.5030,145.8176,-37.4018], async=True)

# Close the log file
#log_file.close()