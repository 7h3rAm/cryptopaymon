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
    utils.info("added data (%dB) to queue: %s (%d total)" % (len(json.loads(data)), self.config["cmdqueue"], utils.queuecount(queuefile=self.config["cmdqueue"])))

  def on_status(self, message):
    utils.info("%s" % message)
    return True

  def on_event(self, message):
    utils.info("%s" % message)
    return True

  def on_direct_message(self, message):
    utils.info("%s" % message)
    return True

  def on_delete(self, message):
    utils.info("%s" % message)
    return True

  def on_limit(self, message):
    utils.info("%s" % message)
    return True

  def on_timeout(self, message):
    utils.info("%s" % message)
    return True

  def on_warning(self, message):
    utils.info("%s" % message)
    return True

  def on_exception(self, message):
    utils.info("%s" % message)
    return True

  def on_error(self, status_code):
    utils.info("%d" % status_code)
    # https://github.com/tweepy/tweepy/blob/master/docs/streaming_how_to.rst
    if status_code == 420:
      utils.info("got status code 420 (rateLimited), disconnecting stream and waiting for %ds." % (self.config["delay"]))
      time.sleep(self.config["delay"])
      utils.info("done sleeping, waiting for stream updates...")
    return True

  def on_error(self, error):
    utils.info("%s" % (error))

class tweetin(object):
  def __init__(self):
    super(tweetin, self).__init__()
    self.conn = utils.create_db("cryptopaymon.sqlite", "schema.sql")
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
    rows = utils.search_db(self.conn, 'SELECT cmdqueue, mentions from config')
    try:
      if rows and rows[0] and len(rows[0]):
        self.config["cmdqueue"], self.config["mentions"] = rows[0][0], rows[0][1].split("|")
      else:
        utils.info("could not load config from database, using old config")
        self.config = copy.deepcopy(oldconfig)
    except:
      self.config = copy.deepcopy(oldconfig)

  def load_apikeys(self):
    oldconfig = copy.deepcopy(self.config)
    rows = utils.search_db(self.conn, 'SELECT twitterconsumerkey, twitterconsumersecret, twitteraccesskey, twitteraccesssecret from apikeys')
    try:
      if rows and rows[0] and len(rows[0]):
        self.config["twitterconsumerkey"], self.config["twitterconsumersecret"], self.config["twitteraccesskey"], self.config["twitteraccesssecret"] = rows[0][0], rows[0][1], rows[0][2], rows[0][3]
      else:
        utils.info("could not load config from database, using old config")
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
      utils.info("starting tweetin module")
      stream.userstream()
    except:
      pass


if __name__ == "__main__":
  tweetin().run()
