# -*- coding: utf-8 -*-

import tweepy
import copy

from pprint import pprint
import json

import utils

class tweetlistener(tweepy.streaming.StreamListener):
  def __init__(self, api, queuefile):
    self.api = api

    self.config = {}
    self.config["cmdqueue"] = queuefile
    self.config["delay"] = 120

    me = self.api.me()
    self.config["you"] = {}
    self.config["you"]["uid"] = me.id
    self.config["you"]["name"] = me.name
    self.config["you"]["username"] = me.screen_name

  def on_data(self, data):
    utils.enqueue(queuefile=self.config["cmdqueue"], data=json.loads(data))
    print("tweetin:on_data: added data (%dB) to queue: %s (%d total)" % (len(json.loads(data)), self.config["cmdqueue"], utils.queuecount(queuefile=self.config["cmdqueue"])))

  def on_status(self, message):
    print("tweetin:on_status: %s" % message)
    return True

  def on_event(self, message):
    print("tweetin:on_event: %s" % message)
    return True

  def on_direct_message(self, message):
    print("tweetin:on_direct_message: %s" % message)
    return True

  def on_delete(self, message):
    print("tweetin:on_delete: %s" % message)
    return True

  def on_limit(self, message):
    print("tweetin:on_limit: %s" % message)
    return True

  def on_timeout(self, message):
    print("tweetin:on_timeout: %s" % message)
    return True

  def on_warning(self, message):
    print("tweetin:on_warning: %s" % message)
    return True

  def on_exception(self, message):
    print("tweetin:on_exception: %s" % message)
    return True

  def on_error(self, status_code):
    print("tweetin:on_error: %d" % status_code)
    # https://github.com/tweepy/tweepy/blob/master/docs/streaming_how_to.rst
    if status_code == 420:
      print("tweetin:on_error: got status code 420 (rateLimited), disconnecting stream and waiting for %ds." % (self.config["delay"]))
      time.sleep(self.config["delay"])
      print("tweetin:on_error: done sleeping, waiting for stream updates...")
    return True

  def on_error(self, error):
    print("tweetin:on_error: %s" % (error))

class tweetin(object):
  def __init__(self, conn):
    super(tweetin, self).__init__()
    self.conn = conn
    self.config = {
      "cmdqueue": None,
      "mentions": None,
      "twitterconsumerkey": None,
      "twitterconsumersecret": None,
      "twitteraccesskey": None,
      "twitteraccesssecret": None
    }

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    details = utils.search_db(self.conn, 'SELECT cmdqueue, mentions from config')
    try:
      if details and details[0] and len(details[0]):
        self.config["cmdqueue"], self.config["mentions"] = details[0][0], details[0][1].split("|")
      else:
        print("tweetin:load_config: could not load config from database, using old config")
        self.config = copy.deepcopy(oldconfig)
    except:
      self.config = copy.deepcopy(oldconfig)

  def load_apikeys(self):
    oldconfig = copy.deepcopy(self.config)
    details = utils.search_db(self.conn, 'SELECT twitterconsumerkey, twitterconsumersecret, twitteraccesskey, twitteraccesssecret from apikeys')
    try:
      if details and details[0] and len(details[0]):
        self.config["twitterconsumerkey"], self.config["twitterconsumersecret"], self.config["twitteraccesskey"], self.config["twitteraccesssecret"] = details[0][0], details[0][1], details[0][2], details[0][3]
      else:
        print("tweetin:load_apikeys: could not load config from database, using old config")
        self.config = copy.deepcopy(oldconfig)
    except:
      self.config = copy.deepcopy(oldconfig)

  def run(self):
    # load config from database
    self.load_config()

    # load apikeys from database
    self.load_apikeys()

    # initialize twiter api
    self.auth = tweepy.OAuthHandler(self.config["twitterconsumerkey"], self.config["twitterconsumersecret"])
    self.auth.secure = True
    self.auth.set_access_token(self.config["twitteraccesskey"], self.config["twitteraccesssecret"])
    self.api = tweepy.API(self.auth)

    try:
      stream = tweepy.Stream(self.api.auth, tweetlistener(self.api, self.config["cmdqueue"]))
      print("tweetin:run: listening for incoming tweets")
      stream.userstream()
    except:
      pass
