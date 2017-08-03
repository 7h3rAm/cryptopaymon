# -*- coding: utf-8 -*-

import tweepy
import copy

from pprint import pprint
import time

import utils

class tweetout(object):
  def __init__(self, conn):
    super(tweetout, self).__init__()
    self.conn = conn
    self.config = {
      "twitterusers": None,
      "tweetqueue": None,
      "queuemonitordelay": None,
      "twitterconsumerkey": None,
      "twitterconsumersecret": None,
      "twitteraccesskey": None,
      "twitteraccesssecret": None
    }
    self.auth = None
    self.api = None

  def send_dm(self, text):
    user = self.config["twitterusers"].split("|")[0]
    self.api.send_direct_message(screen_name=user, text=text)

  def send_tweet(self, message):
    try:
      self.api.update_status(message)
      print("tweetout:send_tweet: tweeted message (%dB)" % (len(message)))
    except:
      self.send_dm(message)
      print("tweetout:send_tweet: exception, sent dm to %s" % (len(text), user))

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    details = utils.search_db(self.conn, 'SELECT authorizedusers, tweetqueue, queuemonitordelay from config')
    try:
      if details and details[0] and len(details[0]):
        self.config["twitterusers"], self.config["tweetqueue"], self.config["queuemonitordelay"] = details[0][0], details[0][1], details[0][2]
      else:
        print("tweetin:load_config: could not load config from database, using old config")
        self.config = copy.deepcopy(oldconfig)
    except:
      self.config = copy.deepcopy(oldconfig)

  def load_apikeys(self):
    details = utils.search_db(self.conn, 'SELECT twitterconsumerkey, twitterconsumersecret, twitteraccesskey, twitteraccesssecret from apikeys')
    if details and details[0] and len(details[0]):
      self.config["twitterconsumerkey"], self.config["twitterconsumersecret"], self.config["twitteraccesskey"], self.config["twitteraccesssecret"] = details[0][0], details[0][1], details[0][2], details[0][3]

  def run(self):
    # load config from database
    self.load_config()

    # load apikeys from database
    self.load_apikeys()

    # initialize twiter api
    self.auth = tweepy.OAuthHandler(self.config["twitterconsumerkey"], self.config["twitterconsumersecret"])
    self.auth.set_access_token(self.config["twitteraccesskey"], self.config["twitteraccesssecret"])
    self.api = tweepy.API(self.auth)

    # monitor tw queue
    print("tweetout:run: monitoring queue: %s" % (self.config["tweetqueue"]))
    try:
      while True:
        count = utils.queuecount(queuefile=self.config["tweetqueue"])
        if count > 0:
          message = utils.dequeue(queuefile=self.config["tweetqueue"])
          self.send_tweet(message)
        time.sleep(self.config["queuemonitordelay"])

        # reload config from database
        self.load_config()
    except:
      import traceback
      traceback.print_exc()
