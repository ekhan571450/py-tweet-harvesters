#!/usr/bin/python

"""
Filename: followers.py

This application harvests tweets.

To configure the environment for this application on Ubuntu 14.04 LTS:
$ sudo apt-get update
$ sudo apt-get install python-pip
$ sudo pip install tweepy
$ sudo pip install couchdb

Documentation for using tweepy and couchdb libraries:
- http://tweepy.readthedocs.io/en/v3.5.0/index.html
- https://pythonhosted.org/CouchDB/
"""

import sys
import argparse
import json
import time
import math
import tweepy
import couchdb

# Create a log file
#log_file = open("message.log","w")
#sys.stdout = log_file

# Parse command line arguments
parser = argparse.ArgumentParser(description='')
parser.add_argument('--id', '-i', type=int, help='The unique node ID: The first node should have an ID of 0')
parser.add_argument('--nodes', '-n', type=int, help='The total number of harvesting nodes')
parser.add_argument('--couchip', '-c', help='The IP address of the CouchDB instance')
parser.add_argument('--consumerkey', '-ck', help='Twitter API Consumer Key')
parser.add_argument('--consumersecret', '-cs', help='Twitter API Consumer Secret')
parser.add_argument('--tokenkey', '-tk', help='Twitter Access Token Key')
parser.add_argument('--tokensecret', '-ts', help='Twitter Access Token Secret')
args = parser.parse_args()

# Initialise Twitter communication
auth = tweepy.OAuthHandler(args.consumerkey, args.consumersecret)
auth.set_access_token(args.tokenkey, args.tokensecret)
try:
  api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
  print "OAuth connection with Twitter established\n"
except:
  print "OAuth connection with Twitter could not be established\n"
  raise

# Initialise CouchDB communication
db_tweets_str = 'tweets'
try:
  couch = couchdb.Server('http://' + args.couchip + ':5984/')
  print "Connected to CouchDB server at http://" + args.couchip + ":5984\n"
except:
  print "Failed to connect to CouchDB server on port 5984 (HTTP)\n"
  raise
try:
  db_tweets = couch[db_tweets_str]
  print "Connected to " + db_tweets_str + " database\n"
except:
  db_tweets = couch.create(db_tweets_str)
  print "Creating new database: " + db_tweets_str + "\n"

# Download list of users from the tweets database
i = 0
queue = []
for row in db_tweets.view('app/users_simple', wrapper=None, group='true'):
  queue.append(row.key)
  i += 1
queue_len = len(queue)

# While (searching)
j = int(math.floor(queue_len / args.nodes) * args.id)
print "Starting at position " + str(j) + " in the queue."
searching = 1
while searching:
  # For each user
  j += 1

  # If the user iterator (j) exceeds a point in the array
  if j > (queue_len - 1):
    # Re-download user queue from tweets database
    i = 0
    queue = []
    for row in db_tweets.view('app/users_simple', wrapper=None, group='true'):
      queue.append(row.key)
      i += 1
    # If the queue has now extended
    if (j <= (len(queue) - 1)):
      # Save a new queue length and continue
      queue_len = len(queue)
    # Otherwise start again at the beginning of the queue
    else:
      j = 0
  
  # Download the followers of the user
  try:
    followers = api.followers(queue[j])
    print "Retrieved followers for user ID " + str(queue[j])
  except:
    # Need error handling if we exceed rate limit (but tweepy will wait on?)
    print "Failed to retrieve followers for user ID " + str(queue[j])
    #time.sleep(900)
    #pass
  
  for follower in followers:
  
    # Download the user_timeline of the current user
    try:
      tweets = api.user_timeline(follower.id_str)
      print "Retrieved tweets for user ID " + follower.id_str
    except:
      # Need error handling if we exceed rate limit (but tweepy will wait on?)
      print "Failed to retrieve tweets for user ID " + follower.id_str
      #time.sleep(900)
      #pass

    for tweet in tweets:
      # Try access place.name (may not exist and will throw an exception)
      try:
        city = tweet.place.name
        if tweet.place.name == 'Melbourne':
          # Store the serialisable JSON data in a string (tweet is a 'Status' object)
          tweet_str = json.dumps(tweet._json)
          # Decode JSON string
          tweet_doc = json.loads(tweet_str)
          # Store the unique tweet ID as the document _id for CouchDB
          tweet_doc.update({'_id': tweet.id_str})
          # Attempt to save tweet to CouchDB
          try:
            db_tweets.save(tweet_doc)
            print "Tweet " + tweet.id_str + " stored in the database."
          except:
            print "Tweet " + tweet.id_str + " already exists in the database."
      except:
        pass
    
    # We are only allowed 180 queries per 15 minutes, so sleep for 5 seconds
    time.sleep(5)

# Close the log file
#log_file.close()