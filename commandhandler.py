# -*- coding: utf-8 -*-

import tweepy

from pprint import pprint
import time
import json
import copy
import sys

import utils

sys.setrecursionlimit(10000)

class commandhandler(object):
  def __init__(self, conn):
    super(commandhandler, self).__init__()
    self.conn = conn
    self.config = {
      "tweetqueue": None,
      "twitteruser": None,
      "cmdqueue": None,
      "mentions": None,
      "authorizedusers": None,
      "queuemonitordelay": 0,
    }
    self.commands = {
      "start": {
        "handler": self.start,
        "authorize": True,
        "dmonly": True,
      },
      "track": {
        "handler": self.start,
        "authorize": True,
        "dmonly": True,
      },
      "stop": {
        "handler": self.stop,
        "authorize": True,
        "dmonly": True,
      },
      "untrack": {
        "handler": self.stop,
        "authorize": True,
        "dmonly": True,
      },
      "show": {
        "handler": self.show,
        "authorize": True,
        "dmonly": True,
      },
      "stats": {
        "handler": self.stats,
        "authorize": False,
        "dmonly": False,
      },
      "compare": {
        "handler": self.compare,
        "authorize": False,
        "dmonly": False,
      },
    }

  def is_authorized(self, sender):
    return sender in self.config["authorizedusers"]

  def start(self, arguments):
    # start <addr> <name1|name2> <#tag1|#tag2> <status>
    try:
      address, names, hashtags, status = arguments.split()
      query = 'SELECT address FROM btcaddresses WHERE address="%s"' % (address)
      details = utils.search_db(self.conn, query)
      if details and len(details):
        query = 'UPDATE btcaddresses SET domonitor=1, dotweet=1 WHERE address="%s"' % (address)
        utils.populate_db(self.conn, query)
        return "commandhandler:start: address %s is already in database as such enabled monitoring" % (address)
      else:
        query = 'INSERT INTO btcaddresses (address, names, hashtags, domonitor, dotweet, status) VALUES ("%s", "%s", "%s", 1, 1, %s)' % (address, names, hashtags, status)
        utils.populate_db(self.conn, query)
        return "commandhandler:start: address %s added for monitoring" % (address)
    except:
      # start <addr|names|hashtags>
      matches = []
      query = 'SELECT address, names, hashtags, balance, dotweet, domonitor, status FROM btcaddresses'
      rows = utils.search_db(self.conn, query)
      if rows and len(rows):
        for row in rows:
          address, names, hashtags = row[0], row[1], row[2]
          if arguments == address or arguments in names.split("|") or arguments in hashtags.split("|"):
            query = 'UPDATE btcaddresses SET domonitor=1, dotweet=1 WHERE address="%s"' % (address)
            utils.populate_db(self.conn, query)
            matches.append(address)
      if len(matches):
        return "commandhandler:start: enabled monitoring for %d entries" % (len(matches))
      else:
        return "commandhandler:start: couldn't find '%s' in database" % (arguments)

  def stop(self, arguments):
    # stop <[addr1] [name1] [addr2] [tag1] [name2]>
    matches = []
    query = 'SELECT address, names, hashtags FROM btcaddresses'
    rows = utils.search_db(self.conn, query)
    if rows and len(rows):
      for row in rows:
        address, names, hashtags = row[0], row[1], row[2]
        for entry in arguments.split():
          if entry == address or entry in names.split("|") or entry in hashtags.split("|"):
            query = 'UPDATE btcaddresses SET domonitor=0, dotweet=0 WHERE address="%s"' % (address)
            utils.populate_db(self.conn, query)
            matches.append(address)
    if len(matches):
      return "commandhandler:stop: stopped monitoring for %d entries" % (len(matches))
    else:
      return "commandhandler:stop: couldn't find '%s' in database" % (arguments)

  def show(self, arguments):
    # show <addr|names|hashtags>
    matches = []
    query = 'SELECT address, names, hashtags, balance, dotweet, domonitor, status FROM btcaddresses'
    rows = utils.search_db(self.conn, query)
    if rows and len(rows):
      for row in rows:
        address, names, hashtags = row[0], row[1], row[2]
        if arguments == address or arguments.lower() in [x.lower() for x in names.split("|")] or arguments.lower() in [x.lower() for x in hashtags.split("|")]:
          matches.append("a:https://blockchain.info/address/%s, n:%s, t:%s, b:%s, dt:%d, dm:%d, s:%d" % (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
    if len(matches):
      return "commandhandler:show:\n%s" % ("\n".join(matches))
    else:
      return "commandhandler:show: couldn't find '%s' in database" % (arguments)

  def stats(self, arguments):
    # stats <addr|names|hashtags>
    matches = []
    query = 'SELECT address, names, hashtags, balance, dotweet, domonitor, status FROM btcaddresses'
    rows = utils.search_db(self.conn, query)
    if rows and len(rows):
      for row in rows:
        address, names, hashtags = row[0], row[1], row[2]
        if arguments == address or arguments.lower() in [x.lower() for x in names.split("|")] or arguments.lower() in [x.lower() for x in hashtags.split("|")]:
          matches.append("a:%s n:%s t:%s b:%s dt:%d dm:%d s:%d" % (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
    if len(matches):
      return "commandhandler:stats:\n%s" % ("\n".join(matches))
    else:
      return "commandhandler:stats: couldn't find '%s' in database" % (arguments)

  def compare(self, arguments):
    return "commandhandler:compare: not implemented"

  def send_dm(sender, text):
    self.api.send_direct_message(screen_name=sender, text=text)

  def parser(self, message):
    if "direct_message" in message:
      if self.config["twitteruser"] != message["direct_message"]["sender"]["screen_name"]:
        sender, dmid, dmtext = message["direct_message"]["sender"]["screen_name"], message["direct_message"]["id"], message["direct_message"]["text"]
        try:
          command, arguments = dmtext.split()[0].lower(), " ".join(dmtext.split()[1:])
          if command in self.commands:
            if self.commands[command]["authorize"]:
              if self.is_authorized(sender):
                print("commandhandler:parser: running command - %s (authorized|user: %s)" % (command, sender))
                msg = self.commands[command]["handler"](arguments)
                if msg:
                  self.send_dm(sender, msg)
                  print("commandhandler:parser: replied to dm (%dB)" % (len(msg)))
                else:
                  self.send_dm(sender, "commandhandler:parser: failed to parse text: %s" % (dmtext))
                  print("commandhandler:parser: failed to parse text: %s" % (dmtext))
              else:
                self.send_dm(sender, "commandhandler:parser: command %s is not authorized for user %s" % (command, sender))
                print("commandhandler:parser: command %s is not authorized for user %s" % (command, sender))
            else:
              print("commandhandler:parser: running command - %s" % (command))
              msg = self.commands[command]["handler"](arguments)
              if msg:
                self.send_dm(sender, msg)
                print("commandhandler:parser: replied to dm (%dB)" % (len(msg)))
              else:
                self.send_dm(sender, "commandhandler:parser: failed to parse text: %s" % (dmtext))
                print("commandhandler:parser: failed to parse text: %s" % (dmtext))
          else:
            self.send_dm(sender, "commandhandler:parser: failed to parse text: %s" % (dmtext))
            print("commandhandler:parser: unknown command - %s" % (command))
        except:
          self.send_dm(sender, "commandhandler:parser: exception while parsing dm '%s'" % (dmtext))
          print("commandhandler:parser: exception while parsing dm: %s" % (message))
          import traceback
          traceback.print_exc()
      else:
        print("commandhandler:parser: ignored dm - self check failed (%s)" % (message["direct_message"]["sender"]["screen_name"]))
    else:
      print("commandhandler:parser: incomplete message (%dB), ignored" % (len(message)))

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    details = utils.search_db(self.conn, 'SELECT twitteruser, tweetqueue, cmdqueue, mentions, authorizedusers, queuemonitordelay from config')
    try:
      if details and details[0] and len(details[0]):
        self.config["twitteruser"], self.config["tweetqueue"], self.config["cmdqueue"], self.config["mentions"], self.config["authorizedusers"], self.config["queuemonitordelay"] = details[0][0], details[0][1], details[0][2], details[0][3].split("|"), details[0][4].split("|"), details[0][5]
      else:
        print("commandhandler:load_config: could not load config from database, using old config")
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

    # monitor txp queue
    print("commandhandler:run: monitoring queue: %s" % (self.config["cmdqueue"]))
    count = utils.queuecount(queuefile=self.config["cmdqueue"])
    while True:
      count = utils.queuecount(queuefile=self.config["cmdqueue"])
      if count > 0:
        message = utils.dequeue(queuefile=self.config["cmdqueue"])
        self.parser(message)
      time.sleep(self.config["queuemonitordelay"])

      # reload config from database
      self.load_config()
