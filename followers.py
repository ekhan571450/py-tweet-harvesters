#!/usr/bin/python3

"""
Filename: followers.py

This application harvests tweets.

To configure the environment for this application (with python3 installed):
$ sudo apt-get update
$ sudo apt-get install python3-pip
$ sudo pip3 install tweepy
$ sudo pip3 install couchdb

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

"""
  Function Definitions
"""

def get_queue ( db_tweets ):
  i = 0
  queue = []
  try:
    for row in db_tweets.view('app/users_simple', wrapper=None, group='true'):
      queue.append(row.key)
      i += 1
    return queue
  except:
    raise Exception('Failed to retrieve user queue')

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
    print ("Tweet " + tweet.id_str + " stored in the database " + str(database.name))
  except:
    print ("Tweet " + tweet.id_str + " already exists in the database " + str(database.name))

"""
  Main Program
"""
 
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
  print ("OAuth connection with Twitter established\n")
except:
  print ("OAuth connection with Twitter could not be established\n")
  raise

# Initialise CouchDB communication
db_tweets_str = 'tweets'
db_tweets_etc_str = 'tweets_etc'
try:
  couch = couchdb.Server('http://' + args.couchip + ':5984/')
  print ("Connected to CouchDB server at http://" + args.couchip + ":5984\n")
except:
  print ("Failed to connect to CouchDB server on port 5984 (HTTP)\n")
  raise
try:
  db_tweets = couch[db_tweets_str]
  print ("Connected to " + db_tweets_str + " database\n")
except:
  db_tweets = couch.create(db_tweets_str)
  print ("Creating new database: " + db_tweets_str + "\n")
try:
  db_tweets_etc = couch[db_tweets_etc_str]
  print ("Connected to " + db_tweets_etc_str + " database\n")
except:
  db_tweets_etc = couch.create(db_tweets_etc_str)
  print ("Creating new database: " + db_tweets_etc_str + "\n")

# Download list of users from the tweets database
queue = get_queue(db_tweets)
queue_len = len(queue)

# While (searching)
j = int(math.floor(queue_len / args.nodes) * args.id)
print ("Starting at position " + str(j) + " in the queue.")
searching = 1
while searching:
  # For each user
  j += 1

  # If the user iterator (j) exceeds a point in the array
  if j > (queue_len - 1):
    # Re-download user queue from tweets database
    queue = get_queue(db_tweets)
    # If the queue has now extended
    if (j <= (len(queue) - 1)):
      # Save a new queue length and continue
      queue_len = len(queue)
    # Otherwise start again at the beginning of the queue
    else:
      j = 0

  # Download the timeline of the user
  try:
    for tweet in tweepy.Cursor(api.user_timeline, id=queue[j]).items():
      # Try access place.name (may not exist and will throw an exception)
      try:
        city = tweet.place.name
        if tweet.place.name == 'Melbourne':
          store_tweet(tweet, db_tweets)
        else:
          store_tweet(tweet, db_tweets_etc)
      except:
        store_tweet(tweet, db_tweets_etc)
        #pass
    print ("Retrieving tweets for user ID " + str(queue[j]))
  except:
    print ("Failed to retrieve tweets for user ID " + str(queue[j]))

  # Download the followers of the user
  try:
    for follower in tweepy.Cursor(api.followers, id=queue[j]).items():
      print ("Retrieving tweets for follower " + str(queue[j]))
      # Download the timeline of the follower
      try:
        for tweet in tweepy.Cursor(api.user_timeline, id=follower.id_str).items():
          # Try access place.name (may not exist and will throw an exception)
          try:
            city = tweet.place.name
            if tweet.place.name == 'Melbourne':
              store_tweet(tweet, db_tweets)
            else:
              store_tweet(tweet, db_tweets_etc)
          except:
            store_tweet(tweet, db_tweets_etc)
            #pass
      except:
        print ("Failed to retrieve tweets for follower " + follower.id_str)

  except:
    print ("Failed to retrieve followers for user ID " + str(queue[j]))

# Close the log file
#log_file.close()