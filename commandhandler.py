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
  def __init__(self):
    super(commandhandler, self).__init__()
    self.conn = utils.create_db("cryptopaymon.sqlite", "schema.sql")
    self.config = {
      "tweetqueue": None,
      "twitteruser": None,
      "cmdqueue": None,
      "mentions": None,
      "rootuser": None,
      "authorizedusers": None,
      "queuemonitordelay": 0,
      "statuses": {
        "unknown": 0,
        "good": 1,
        "bad": 2
      },
      "exchangerates": {
        "btc2usd": 0
      },
    }
    self.commands = {
      "add": {
        "user": "root",
        "usage": "add address name1|name2 #hashtag1|#hashtag2 good|bad|unknown",
        "help": "will add new entry and enable tracking|tweeting|txstats by default",
        "handler": self.add,
      },
      "remove": {
        "user": "root",
        "usage": "remove address|name1|hashtag2",
        "help": "will loop over all addresses and remove those that match params",
        "handler": self.remove,
      },
      "txtrack": {
        "user": "root",
        "usage": "txtrack start|stop address|name1|hashtag2",
        "help": "will loop over all addresses and enable|disable tracking live txs for those that match params",
        "handler": self.txtrack,
      },
      "txtweet": {
        "user": "root",
        "usage": "txtweet start|stop address|name1|hashtag2",
        "help": "will loop over all addresses and enable|disable tweeting live txs for those that match params",
        "handler": self.txtweet,
      },
      "txstats": {
        "user": "root",
        "usage": "txstats start|stop address|name1|hashtag2",
        "help": "will loop over all addresses and enable|disable stats tweet for those that match params",
        "handler": self.txstats,
      },
      "auth": {
        "user": "root",
        "usage": "auth add|remove handle",
        "help": "will add|remove handle in authorized users list",
        "handler": self.auth,
      },
      "update": {
        "user": "root",
        "usage": "update address|name1|hashtag2 good|bad|unknown",
        "help": "will loop over all addresses and update status for those that match params",
        "handler": self.update,
      },
      "show": {
        "user": "root, auth",
        "usage": "show address|name1|hashtag2",
        "help": "will loop over all addresses and generate combined stats for those that match params",
        "handler": self.show,
      },
      "show": {
        "user": "root, auth",
        "usage": "show address|name1|hashtag2",
        "help": "will loop over all addresses and generate combined stats for those that match params",
        "handler": self.show,
      },
      "help": {
        "user": "root, auth",
        "usage": "help",
        "help": "will show help for available commands",
        "handler": self.help,
      },
    }
    self.error = None

  def is_root(self, sender):
    return sender == self.config["rootuser"]

  def is_authorized(self, sender):
    return sender in self.config["authorizedusers"]

  def help(self, sender, arguments):
    """
    user: root, auth
    help
    will show help for available commands
    """
    if self.is_root(sender) or self.is_authorized(sender):
      if arguments and arguments != "":
        if arguments.lower() in self.commands:
          result = list()
          result.append("Command: %s" % (arguments.lower()))
          result.append("User: %s" % (self.commands[arguments.lower()]["user"]))
          result.append("Usage: %s" % (self.commands[arguments.lower()]["usage"]))
          result.append("Help: %s" % (self.commands[arguments.lower()]["help"]))
          return "\n".join(result)
        else:
          self.error = "no such command: %s" % (arguments)
          return None
      else:
        result = list()
        for idx, cmd in enumerate(self.commands):
          result.append("(%d) Command: %s" % (idx+1, cmd))
          result.append("User: %s" % (self.commands[cmd]["user"]))
          result.append("Usage: %s" % (self.commands[cmd]["usage"]))
          result.append("Help: %s" % (self.commands[cmd]["help"]))
          result.append("---")
        return "\n".join(result)
    else:
      self.error = "user %s is not authorized" % (sender)
      return None

  def add(self, sender, arguments):
    """
    user: root
    add address name1|name2 #hashtag1|#hashtag2 good|bad|unknown
    will add new entry and enable tracking|tweeting|txstats by default
    """
    if self.is_root(sender):
      try:
        address, names, hashtags, status = arguments.split()
        rows = utils.search_db(self.conn, 'SELECT address, names, hashtags FROM btcaddresses WHERE address="%s"' % (address))
        if rows and len(rows):
          self.error = "address %s already in database"  % (address)
          return None
        else:
          query = 'INSERT INTO btcaddresses (address, names, hashtags, status) VALUES ("%s", "%s", "%s", %d)' % (address, names, hashtags, self.config["statuses"][status.lower()] if status.lower() in self.config["statuses"] else 0)
          utils.populate_db(self.conn, query)
          return "added %s to database (%s, %s, %s)" % (address, names, hashtags, status.lower())
      except:
        self.error = "incorrect params for this command"
        return None
    else:
      self.error = "user %s is not authorized" % (sender)
      return None

  def remove(self, sender, arguments):
    """
    user: root
    remove address|name1|hashtag2
    will loop over all addresses and remove those that match params
    """
    if self.is_root(sender):
      try:
        rows = utils.search_db(self.conn, 'SELECT address, names, hashtags FROM btcaddresses')
        rmcount = 0
        for row in rows:
          if arguments == row[0] or arguments.lower() in row[1].lower() or arguments.lower() in row[2].lower():
            query = 'DELETE FROM btcaddresses WHERE address="%s"' % (row[0])
            utils.populate_db(self.conn, query)
            rmcount += 1
        if rmcount:
          return "deleted %d rows matching pattern: %s" % (rmcount, arguments)
        else:
          self.error = "could not find any rows matching pattern: %s" % (arguments)
          return None
      except:
        import traceback
        traceback.print_exc()
        self.error = "incorrect params for this command: remove %s" % (arguments)
        return None
    else:
      self.error = "user %s is not authorized" % (sender)
      return None

  def txtrack(self, sender, arguments):
    """
    user: root
    txtrack start|stop address|name1|hashtag2
    will loop over all addresses and enable|disable tracking live txs for those that match params
    """
    if self.is_root(sender):
      try:
        cmd, pattern = arguments.split()
        if cmd.lower() in ["start", "stop"]:
          rows = utils.search_db(self.conn, 'SELECT address, names, hashtags FROM btcaddresses')
          trkcount = 0
          for row in rows:
            if pattern == row[0] or pattern.lower() in row[1].lower() or pattern.lower() in row[2].lower():
              if cmd.lower() == "start":
                query = 'UPDATE btcaddresses SET domonitor=1 WHERE address="%s"' % (row[0])
                utils.populate_db(self.conn, query)
                trkcount += 1
              else:
                query = 'UPDATE btcaddresses SET domonitor=0 WHERE address="%s"' % (row[0])
                utils.populate_db(self.conn, query)
                trkcount += 1
          if trkcount:
            return "updated %d rows matching pattern: %s" % (trkcount, pattern)
          else:
            self.error = "could not find any rows matching pattern: %s" % (pattern)
            return None
        else:
          self.error = "incorrect subcommand for this command: txtrack %s" % (arguments)
      except:
        import traceback
        traceback.print_exc()
        self.error = "incorrect params for this command: txtrack %s" % (arguments)
        return None
    else:
      self.error = "user %s is not authorized" % (sender)
      return None

  def txtweet(self, sender, arguments):
    """
    user: root
    txtweet start|stop address|name1|hashtag2
    will loop over all addresses and enable|disable tweeting live txs for those that match params
    """
    if self.is_root(sender):
      try:
        cmd, pattern = arguments.split()
        if cmd.lower() in ["start", "stop"]:
          rows = utils.search_db(self.conn, 'SELECT address, names, hashtags FROM btcaddresses')
          trkcount = 0
          for row in rows:
            if pattern == row[0] or pattern.lower() in row[1].lower() or pattern.lower() in row[2].lower():
              if cmd.lower() == "start":
                query = 'UPDATE btcaddresses SET dotweet=1 WHERE address="%s"' % (row[0])
                utils.populate_db(self.conn, query)
                trkcount += 1
              else:
                query = 'UPDATE btcaddresses SET dotweet=0 WHERE address="%s"' % (row[0])
                utils.populate_db(self.conn, query)
                trkcount += 1
          if trkcount:
            return "updated %d rows matching pattern: %s" % (trkcount, pattern)
          else:
            self.error = "could not find any rows matching pattern: %s" % (pattern)
            return None
        else:
          self.error = "incorrect subcommand for this command: txtweet %s" % (arguments)
      except:
        import traceback
        traceback.print_exc()
        self.error = "incorrect params for this command: txtweet %s" % (arguments)
        return None
    else:
      self.error = "user %s is not authorized" % (sender)
      return None

  def txstats(self, sender, arguments):
    """
    user: root
    txstats start|stop address|name1|hashtag2
    will loop over all addresses and enable|disable stats tweet for those that match params
    """
    if self.is_root(sender):
      try:
        cmd, pattern = arguments.split()
        if cmd.lower() in ["start", "stop"]:
          rows = utils.search_db(self.conn, 'SELECT address, names, hashtags FROM btcaddresses')
          trkcount = 0
          for row in rows:
            if pattern == row[0] or pattern.lower() in row[1].lower() or pattern.lower() in row[2].lower():
              if cmd.lower() == "start":
                query = 'UPDATE btcaddresses SET dostats=1 WHERE address="%s"' % (row[0])
                utils.populate_db(self.conn, query)
                trkcount += 1
              else:
                query = 'UPDATE btcaddresses SET dostats=0 WHERE address="%s"' % (row[0])
                utils.populate_db(self.conn, query)
                trkcount += 1
          if trkcount:
            return "updated %d rows matching pattern: %s" % (trkcount, pattern)
          else:
            self.error = "could not find any rows matching pattern: %s" % (pattern)
            return None
        else:
          self.error = "incorrect subcommand for this command: txstats %s" % (arguments)
      except:
        import traceback
        traceback.print_exc()
        self.error = "incorrect params for this command: txstats %s" % (arguments)
        return None
    else:
      self.error = "user %s is not authorized" % (sender)
      return None

  def auth(self, sender, arguments):
    """
    user: root
    auth add|remove handle
    will add|remove handle in authorized users list
    """
    if self.is_root(sender):
      try:
        cmd, handle = arguments.split()
        if cmd.lower() in ["add", "remove"]:
          if cmd.lower() == "add":
            rows = utils.search_db(self.conn, 'SELECT authorizedusers FROM config')
            authusers = rows[0][0]
            if handle not in authusers:
              authusers = authusers.split("|")
              authusers.append(handle)
              query = 'UPDATE config SET authorizedusers="%s"' % ("|".join(authusers))
              utils.populate_db(self.conn, query)
              return "user %s is authorized now" % (handle)
            else:
              self.error = "user %s already authorized" % (handle)
              return None
          else:
            rows = utils.search_db(self.conn, 'SELECT authorizedusers FROM config')
            authusers = rows[0][0]
            if handle in authusers:
              authusers = authusers.split("|")
              authusers.remove(handle)
              query = 'UPDATE config SET authorizedusers="%s"' % ("|".join(authusers))
              utils.populate_db(self.conn, query)
              return "user %s is not authorized now" % (handle)
            else:
              self.error = "user %s is not authorized" % (handle)
              return None
        else:
          self.error = "incorrect subcommand for this command: auth %s" % (arguments)
      except:
        import traceback
        traceback.print_exc()
        self.error = "incorrect params for this command: auth %s" % (arguments)
        return None
    else:
      self.error = "user %s is not authorized" % (sender)
      return None

  def update(self, sender, arguments):
    """
    user: root
    update address|name1|hashtag2 good|bad|unknown
    will loop over all addresses and update status for those that match params
    """
    if self.is_root(sender):
      try:
        pattern, status = arguments.split()
        if status.lower() in self.config["statuses"]:
          rows = utils.search_db(self.conn, 'SELECT address, names, hashtags FROM btcaddresses')
          upcount = 0
          for row in rows:
            if pattern == row[0] or pattern.lower() in row[1].lower() or pattern.lower() in row[2].lower():
              query = 'UPDATE btcaddresses SET status=%d WHERE address="%s"' % (self.config["statuses"][status.lower()], row[0])
              utils.populate_db(self.conn, query)
              upcount += 1
          if upcount:
            return "updated %d rows matching pattern: %s" % (upcount, pattern)
          else:
            self.error = "could not find any rows matching pattern: %s" % (pattern)
            return None
        else:
          self.error = "incorrect status: %s" % (status)
          return None
      except:
        import traceback
        traceback.print_exc()
        self.error = "incorrect params for this command: update %s" % (arguments)
        return None
    else:
      self.error = "user %s is not authorized" % (sender)
      return None

  def show(self, sender, arguments):
    """
    user: root, auth
    show address|name1|hashtag2
    will loop over all addresses and generate combined stats for those that match params
    """
    if self.is_root(sender) or self.is_authorized(sender):
      try:
        # load exchange rates from stats table
        rows = utils.search_db(self.conn, 'SELECT btc2usd FROM forex ORDER BY fid DESC LIMIT 1')
        self.config["exchangerates"]["btc2usd"] = rows[0][0]
        result, summary, allcount, mtcount, txs, rcvd, sent, balance, lasttx_epoch, lasttx_human = list(), list(), 0, 0, 0, 0, 0, 0, 0, None
        skipargscheck = False
        if arguments.lower() == "all":
          rows = utils.search_db(self.conn, 'SELECT address, names, hashtags, txs, rcvd, sent, balance, lasttx_epoch, lasttx_human FROM btcaddresses')
          skipargscheck = True
        elif arguments.lower() == "bad":
          rows = utils.search_db(self.conn, 'SELECT address, names, hashtags, txs, rcvd, sent, balance, lasttx_epoch, lasttx_human FROM btcaddresses WHERE status=2')
          skipargscheck = True
        elif arguments.lower() == "good":
          rows = utils.search_db(self.conn, 'SELECT address, names, hashtags, txs, rcvd, sent, balance, lasttx_epoch, lasttx_human FROM btcaddresses WHERE status=1')
          skipargscheck = True
        elif arguments.lower() == "unknown":
          rows = utils.search_db(self.conn, 'SELECT address, names, hashtags, txs, rcvd, sent, balance, lasttx_epoch, lasttx_human FROM btcaddresses WHERE status=0')
          skipargscheck = True
        if skipargscheck:
          for row in rows:
            summary.append("Address: %s" % (row[0]))
            summary.append("Names: %s" % (row[1]))
            summary.append("Hashtags: %s" % (row[2]))
            summary.append("Transactions: %s" % (row[3]))
            summary.append("Received: %.2f (%.2f USD)" % (row[4], row[4]*self.config["exchangerates"]["btc2usd"]))
            summary.append("Sent: %.2f (%.2f USD)" % (row[5], row[5]*self.config["exchangerates"]["btc2usd"]))
            summary.append("Balance: %.2f (%.2f USD)" % (row[6], row[6]*self.config["exchangerates"]["btc2usd"]))
            summary.append("Last TX: %s" % (row[8]))
            allcount += 1
            # count all txs (value should be >= 0, -1 is used as default while populating database initially)
            if row[3] >= 0:
              mtcount += 1
            # use only those addresses that have atleast 1 or more txs
            if row[3] > 0:
              txs += row[3]
              rcvd += row[4]
              sent += row[5]
              balance += row[6]
            summary.append("---")
        else:
          rows = utils.search_db(self.conn, 'SELECT address, names, hashtags, txs, rcvd, sent, balance, lasttx_epoch, lasttx_human FROM btcaddresses')
          for row in rows:
            if arguments == row[0] or arguments.lower() in row[1].lower() or arguments.lower() in row[2].lower():
              summary.append("Address: %s" % (row[0]))
              summary.append("Names: %s" % (row[1]))
              summary.append("Hashtags: %s" % (row[2]))
              summary.append("Transactions: %s" % (row[3]))
              summary.append("Received: %.2f (%.2f USD)" % (row[4], row[4]*self.config["exchangerates"]["btc2usd"]))
              summary.append("Sent: %.2f (%.2f USD)" % (row[5], row[5]*self.config["exchangerates"]["btc2usd"]))
              summary.append("Balance: %.2f (%.2f USD)" % (row[6], row[6]*self.config["exchangerates"]["btc2usd"]))
              summary.append("Last TX: %s" % (row[8]))
              allcount += 1
              # count all txs (value should be >= 0, -1 is used as default while populating database initially)
              if row[3] >= 0:
                mtcount += 1
              # use only those addresses that have atleast 1 or more txs
              if row[3] > 0:
                txs += row[3]
                rcvd += row[4]
                sent += row[5]
                balance += row[6]
              summary.append("---")
        summary.append("\n")
        summary.append("Matches: %d" % (mtcount))
        summary.append("Transactions: %d" % (txs))
        summary.append("Received: %.2f (%.2f USD)" % (rcvd, rcvd*self.config["exchangerates"]["btc2usd"] if rcvd >= 0 else -1))
        summary.append("Sent: %.2f (%.2f USD)" % (sent, sent*self.config["exchangerates"]["btc2usd"] if sent >= 0 else -1))
        summary.append("Balance: %.2f (%.2f USD)" % (balance, balance*self.config["exchangerates"]["btc2usd"] if balance >= 0 else -1))
        if mtcount > 0:
          return "\n".join(summary)
        elif allcount > 0:
          self.error = "could not find any txs for pattern: %s" % (arguments)
          return None
        else:
          self.error = "could not find any rows matching pattern: %s" % (arguments)
          return None
      except:
        import traceback
        traceback.print_exc()
        self.error = "incorrect params for this command: show %s" % (arguments)
        return None
    else:
      self.error = "user %s is not authorized" % (sender)
      return None

  def send_dm(self, sender, text):
    for chunk in utils.split_twitter_text(text=text, maxchars=1000):
      self.api.send_direct_message(screen_name=sender, text=chunk)

  def parser(self, message):
    if "direct_message" in message:
      if self.config["twitteruser"] != message["direct_message"]["sender"]["screen_name"]:
        sender, dmid, dmtext = message["direct_message"]["sender"]["screen_name"], message["direct_message"]["id"], message["direct_message"]["text"]
        try:
          command, arguments = dmtext.split()[0].lower(), " ".join(dmtext.split()[1:])
          if command in self.commands:
            msg = self.commands[command]["handler"](sender, arguments)
            if msg:
              self.send_dm(sender, msg)
              utils.info("replied to dm (%dB)" % (len(msg)))
            else:
              self.send_dm(sender, self.error)
              utils.info(self.error)
          else:
            self.send_dm(sender, "failed to parse text: %s" % (dmtext))
            utils.info("failed to parse text: %s" % (dmtext))
        except:
          self.send_dm(sender, "exception while parsing dm: '%s'" % (dmtext))
          utils.info("exception while parsing dm: %s" % (dmtext))
          import traceback
          traceback.print_exc()
      else:
        utils.info("ignored dm - self check failed (%s)" % (message["direct_message"]["sender"]["screen_name"]))
    else:
      utils.info("incomplete message (%dB), ignored" % (len(message)))

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    rows = utils.search_db(self.conn, 'SELECT twitteruser, tweetqueue, cmdqueue, mentions, authorizedusers, queuemonitordelay, rootuser from config')
    try:
      if rows and rows[0] and len(rows[0]):
        self.config["twitteruser"], self.config["tweetqueue"], self.config["cmdqueue"], self.config["mentions"], self.config["authorizedusers"], self.config["queuemonitordelay"], self.config["rootuser"] = rows[0][0], rows[0][1], rows[0][2], rows[0][3].split("|"), rows[0][4].split("|"), rows[0][5], rows[0][6]
      else:
        utils.info("could not load config from database, using old config")
        self.config = copy.deepcopy(oldconfig)
    except:
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

    # monitor txp queue
    utils.info("starting commandhandler module (%s)" % (self.config["cmdqueue"]))
    count = utils.queuecount(queuefile=self.config["cmdqueue"])
    while True:
      count = utils.queuecount(queuefile=self.config["cmdqueue"])
      if count > 0:
        message = utils.dequeue(queuefile=self.config["cmdqueue"])
        self.parser(message)
      time.sleep(self.config["queuemonitordelay"])

      # reload config from database
      self.load_config()


if __name__ == "__main__":
  commandhandler().run()
