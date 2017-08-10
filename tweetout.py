# -*- coding: utf-8 -*-

import tweepy
import copy

from pprint import pprint
import time

import utils

class tweetout(object):
  def __init__(self):
    super(tweetout, self).__init__()
    self.conn = utils.create_db("cryptopaymon.sqlite", "schema.sql")
    self.config = {
      "twitterusers": None,
      "tweetqueue": None,
      "tweetmediaqueue": None,
      "tweetdelay": None,
      "twitterconsumerkey": None,
      "twitterconsumersecret": None,
      "twitteraccesskey": None,
      "twitteraccesssecret": None,
      "generichashtags": None,
      "tmpfile": "/tmp/.imgfile.png",
    }
    self.auth = None
    self.api = None

  def send_dm(self, text):
    user = self.config["twitterusers"].split("|")[0]
    self.api.send_direct_message(screen_name=user, text=text)

  def send_tweet_media(self, imgdata):
    try:
      with open(self.config["tmpfile"], "wb") as tmpfo:
        tmpfo.write(imgdata)
      self.api.update_with_media(self.config["tmpfile"], status=self.config["generichashtags"])
      utils.info("tweeted media (%s)" % (self.config["tmpfile"]))
      utils.remove_file(self.config["tmpfile"])
    except tweepy.error.TweepError as ex:
      utils.warn(ex)
      utils.enqueue(queuefile=self.config["tweetmediaqueue"], data=imgdata)
      time.sleep(self.config["tweetdelay"])
    except:
      import traceback
      traceback.print_exc()
      self.send_dm("exception while sending media tweet: %dB" % (len(imgdata)))
      utils.warn("exception, sent dm to %s" % (self.config["twitterusers"].split("|")[0]))
      utils.remove_file(self.config["tmpfile"])

  def send_tweet(self, message):
    try:
      self.api.update_status(message)
      utils.info("tweeted message (%dB)" % (len(message)))
    except tweepy.error.TweepError as ex:
      utils.warn(ex)
      utils.enqueue(queuefile=self.config["tweetqueue"], data=message)
      time.sleep(self.config["tweetdelay"])
    except:
      import traceback
      traceback.print_exc()
      self.send_dm("exception while sending tweet: %s" % (message))
      utils.warn("exception, sent dm to %s" % (self.config["twitterusers"].split("|")[0]))

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    rows = utils.search_db(self.conn, 'SELECT authorizedusers, tweetqueue, tweetmediaqueue, generichashtags, tweetdelay from config')
    try:
      if rows and rows[0] and len(rows[0]):
        self.config["twitterusers"], self.config["tweetqueue"], self.config["tweetmediaqueue"], self.config["generichashtags"], self.config["tweetdelay"] = rows[0][0], rows[0][1], rows[0][2], rows[0][3], rows[0][4]
      else:
        utils.info("could not load config from database, using old config")
        self.config = copy.deepcopy(oldconfig)
    except:
      import traceback
      traceback.print_exc()
      self.config = copy.deepcopy(oldconfig)

  def load_apikeys(self):
    rows = utils.search_db(self.conn, 'SELECT twitterconsumerkey, twitterconsumersecret, twitteraccesskey, twitteraccesssecret from apikeys')
    if rows and rows[0] and len(rows[0]):
      self.config["twitterconsumerkey"], self.config["twitterconsumersecret"], self.config["twitteraccesskey"], self.config["twitteraccesssecret"] = rows[0][0], rows[0][1], rows[0][2], rows[0][3]

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
    utils.info("starting tweetout module (%s, %s)" % (self.config["tweetqueue"], self.config["tweetmediaqueue"]))
    try:
      while True:
        count = utils.queuecount(queuefile=self.config["tweetqueue"])
        if count > 0:
          message = utils.dequeue(queuefile=self.config["tweetqueue"])
          self.send_tweet(message)
        count = utils.queuecount(queuefile=self.config["tweetmediaqueue"])
        if count > 0:
          imgdata = utils.dequeue(queuefile=self.config["tweetmediaqueue"])
          self.send_tweet_media(imgdata)
        time.sleep(self.config["tweetdelay"])

        # reload config from database
        self.load_config()
    except:
      import traceback
      traceback.print_exc()
      pass


if __name__ == "__main__":
  tweetout().run()
