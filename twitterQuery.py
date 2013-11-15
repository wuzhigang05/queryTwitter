#!/usr/bin/env python
import twitter
from pandas import *
import json
import sys
import pdb
import argparse
from datetime import datetime
import sqlite3
import os
import shutil

def authentication():
  """
  load authentication info and return Twitter Api

  Parameters:
  ----------
  None

  Return:
  --------
  api object

  """ 
  pathToConfig = os.path.join(prefix, "twitterConfig")
  config = json.load(open(pathToConfig))
  consumer_key = config['consumer_key']
  consumer_secret = config['consumer_secret']
  access_token = config['access_token']
  access_token_secret = config['access_token_secret']
  api = twitter.Api(consumer_key=consumer_key, consumer_secret=consumer_secret,
      access_token_key=access_token, access_token_secret=access_token_secret)
  return api

def getTrends():
  """
  return a list of top 10 worldwidely most popular hash tags on twitter

  Parameters:
  ----------
  None

  Return:
  -------
  A list of string tweets, representing current trend

  """ 
  api = authentication()
  names = [i.name for i in api.GetTrendsCurrent()]
  stringTrends = [i.strip('#') for i in names and ]
  trends = [i for i in stringTrends if i != ""]
  return trends

def queryTerm2Twitter(term):
  """
  gvien a term, this function will do a search and return the `timestamp`  
  for the oldest tweet that mentioned this term

  Parameters:
  ----------
  term: str, e.g. "onescreen"

  Return:
  -------
  The timeStamp object of the oldest tweet
  """ 
  statusList = api.GetSearch(term, count=100, result_type='recent')
  timeStampOfStatus = [datetime.fromtimestamp(i.created_at_in_seconds) for i in statusList]
  timeStampOfStatus.sort() 
  return timeStampOfStatus[0]
   
def populateSQlite(tagDf):
  """
  dump the DataFrame into sqlite3

  Parameters:
  ----------
  tagDf: a pandas `DataFrame` object

  Return:
  -------
  None
  """ 
  conn = sqlite3.connect(os.path.join(prefix, args.db))
  with conn:
    cur = conn.cursor()
    cmds = ['INSERT INTO value VALUES(%d, \"%s\", %d);' % (r[0], r[1], r[2]) for i, r in tagDf.iterrows()]
    cmds = "\n".join(cmds)
    cur.executescript(cmds)
    conn.commit()

def loadValueTableFromSqlite():
  """
  The populateSQlite is function you store data in `DataFrame` into sqlite db process. 
  This function does opposite to that function, which retrieving the data from sqlite3 
  database and then stored into DataFrame

  Parameters:
  ----------
  None

  Return:
  -------
  a `DataFrame`

  """ 
  conn = sqlite3.connect(prefix + args.db)
  df = io.read_frame("SELECT * FROM value", conn) 
  return df


if __name__ == '__main__': 
  o = sys.stdout
  e = sys.stderr
  parser= argparse.ArgumentParser(
      description="This program will run a few queries against the twitter using " +
  "twitter-api with different terms, which covers several popular topic, eg. " +
  "programming language, politics and editors. " +
  "The program dumps each query's result into a SQLite database.")
  parser.add_argument("--hashtag", 
      help="The name of the table only stores the hashtags. Default: hashtag", 
      default='hashtag')
  parser.add_argument("--value", 
      help="The name of the table,which contains hashtag, time and value. Default: 'value'", 
      default='value')
  parser.add_argument("--db", 
      help="The name of the SQLite3 database. Default: hachathon.db", 
      default='hackathon.db')

  args = parser.parse_args()
  prefix = os.path.dirname(sys.argv[0])
  api = authentication()

  # Search terms are divided into different categories so that they reflect different interests 
  terms = ['python', 'java', 'ruby', # programming
      'sublime', 'vim', 'emacs', # editor tool
      'cascading', 'scalding', # library
      'insurance', 'government', #politics
      'nsa', 'bitcoin', # ..
      'california','onescreen', 
      'lol', 'omg'] # LOL
  currentTimeObj = datetime.now()
  currentTimeStr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  currentTimeList = [currentTimeStr] * len(terms)

  TweetsTimeStamps = [queryTerm2Twitter(i) for i in terms]

  # write some logging data 
  for term, timeStamp in zip(terms, TweetsTimeStamps):
    print "%s => %s" % (term, timeStamp.strftime("%Y-%m-%d %H:%M:%S"))
    print "%s => %d" % (term, (currentTimeObj-timeStamp).total_seconds())
  
  values = [1000/((currentTimeObj - i).total_seconds()/60.0) for i in TweetsTimeStamps] 
  dfCurrent = DataFrame({'hashTag': range(1, len(terms) + 1), 'timeStamp':currentTimeList, 'value':values})

  hashTagDf = DataFrame({'id': range(1, len(terms) + 1), 'name': terms})
  
  conn = sqlite3.connect(os.path.join(prefix, args.db))
  with conn:
    dfCurrent.to_sql(args.value, conn, flavor='sqlite', if_exists='append')
  conn.close()
  
  
