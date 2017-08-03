# -*- coding: utf-8 -*-

from pprint import pprint
import random
import time
import copy

import utils

class txparser(object):
  def __init__(self, conn):
    super(txparser, self).__init__()
    self.conn = conn
    self.config = {
      "taskqueue": None,
      "tweetqueue": None,
      "generichashtags": None,
      "ignorehashtags": None,
      "happyemojis": None,
      "neutralemojis": None,
      "sademojis": None,
      "queuemonitordelay": 0,
      "satoshi2btc": 1e8,
      "exchangerates": {
        "btc2usd": 0
      }
    }

  def update_balance(self, address):
    balance = utils.btc_balance(address)
    balance /= self.config["satoshi2btc"]
    query = 'UPDATE btcaddresses SET balance=%f WHERE address="%s"' % (balance, address)
    utils.populate_db(self.conn, query)

  def parser(self, message):
    # load exchange rates from stats table
    query = 'SELECT btc2usd FROM forex ORDER BY fid DESC LIMIT 1'
    details = utils.search_db(self.conn, query)
    self.config["exchangerates"]["btc2usd"] = details[0][0]

    # dict to store tx information
    txinfo = {
      "source": {},
      "destination": {},
      "txhash": message["x"]["hash"],
      "timestamp_epoch": message["x"]["time"],
      "timestamp_human": utils.epoch_to_human_utc(message["x"]["time"]),
      "relayip": message["x"]["relayed_by"],
      "rcvd": 0,
      "sent": 0,
    }

    # load source addresses and btc values
    for entry in message["x"]["inputs"]:
      if entry["prev_out"]["addr"] is not None and entry["prev_out"]["addr"] not in txinfo["source"]:
        txinfo["source"][entry["prev_out"]["addr"]] = entry["prev_out"]["value"]/self.config["satoshi2btc"]
        if utils.search_db(self.conn, 'SELECT address FROM btcaddresses WHERE address="%s"' % (entry["prev_out"]["addr"])):
          txinfo["sent"] = 1

    # load destination addresses and btc values
    for entry in message["x"]["out"]:
      if entry["addr"] is not None and entry["addr"] not in txinfo["destination"]:
        txinfo["destination"][entry["addr"]] = entry["value"]/self.config["satoshi2btc"]
        if utils.search_db(self.conn, 'SELECT address FROM btcaddresses WHERE address="%s"' % (entry["addr"])):
          txinfo["rcvd"] = 1

    # a tx with sent and rcvd both set has to be ignored, for now
    if txinfo["sent"] == 1 and txinfo["rcvd"] == 1:
      print("txparser:parser: ignored txhash https://blockchain.info/tx/%s" % (txinfo["txhash"]))
      return

    # update btcaddresses table with in|out address information
    alladdresses = utils.all_dict_keys([txinfo["source"], txinfo["destination"]])
    for address in alladdresses:
      query = 'SELECT inaddresses, outaddresses FROM btcaddresses WHERE address="%s"' % (address)
      details = utils.search_db(self.conn, query)
      if details and len(details) and details[0] and len(details[0]):
        if details[0][0]:
          inaddrs = "|".join(list(set(list(txinfo["source"].keys()) + [details[0][0]])))
        else:
          inaddrs = "|".join(list(set(txinfo["source"].keys())))
        if details[0][1]:
          outaddrs = "|".join(list(set(list(txinfo["destination"].keys()) + [details[0][1]])))
        else:
          outaddrs = "|".join(list(set(txinfo["destination"].keys())))
        query = 'UPDATE btcaddresses SET inaddresses="%s", outaddresses="%s" WHERE address="%s"' % (inaddrs, outaddrs, address)
        utils.populate_db(self.conn, query)

    # update btctransactions table
    amountbtc, address, senders = 0, None, []
    senders = list(txinfo["source"].keys())
    for addr in txinfo["source"].keys():
      if utils.search_db(self.conn, 'SELECT address FROM btcaddresses WHERE address="%s"' % (addr)):
        address = addr
        amountbtc = txinfo["source"][addr]
        break
    receivers = list(txinfo["destination"].keys())
    for addr in txinfo["destination"].keys():
      if utils.search_db(self.conn, 'SELECT address FROM btcaddresses WHERE address="%s"' % (addr)):
        address = addr
        amountbtc = txinfo["destination"][addr]
        break
    query = 'INSERT INTO btctransactions (txhash, address, timestamp_epoch, timestamp_human, amountbtc, relayip, rcvd, sent, receivers, senders) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' % (
      txinfo["txhash"],
      address if address else "",
      txinfo["timestamp_epoch"],
      txinfo["timestamp_human"],
      amountbtc,
      txinfo["relayip"],
      txinfo["rcvd"],
      txinfo["sent"],
      "|".join(receivers) if receivers and len(receivers) else "",
      "|".join(senders) if senders and len(senders) else "",
    )
    utils.populate_db(self.conn, query)
    print("txparser:parser: added transaction details to localdb (sent: %s, rcvd: %s)" % (True if txinfo["sent"] else False, True if txinfo["rcvd"] else False))

    # add tweet message to tw queue
    tweet = []
    for address in txinfo["source"].keys():
      details = utils.search_db(self.conn, 'SELECT names, balance, hashtags, status FROM btcaddresses WHERE address="%s" AND dotweet=1' % (address))
      if details and len(details) and details[0] and len(details[0]):
        names, balance, hashtags, status = details[0][0], details[0][1], details[0][2], details[0][3]
        self.update_balance(address)
        balance = balance*self.config["exchangerates"]["btc2usd"] if balance >= 0 else -1
        for tag in self.config["ignorehashtags"].split("|"):
          hashtags = hashtags.replace(tag, "")
        hashtags = hashtags.strip("|")
        sender = None
        if hashtags and hashtags != "":
          sender = hashtags
        elif names and names != "":
          sender = "https://blockchain.info/address/%s (%s)" % (address, names)
        else:
          sender = "https://blockchain.info/address/%s" % (address)
        if status == 0:
          emoji = random.choice(self.config["neutralemojis"].split("|"))
        elif status == 1:
          emoji = random.choice(self.config["happyemojis"].split("|"))
        elif status == 2:
          emoji = random.choice(self.config["sademojis"].split("|"))
        else:
          emoji = random.choice(self.config["neutralemojis"])
        tweet.append("%s sent %f BTC ($%.2f) (https://blockchain.info/tx/%s) %s %s" % (
          sender,
          txinfo["source"][address],
          txinfo["source"][address]*self.config["exchangerates"]["btc2usd"],
          txinfo["txhash"],
          #"(balance: %.02f) " % balance if balance >= 0 else "",
          self.config["generichashtags"],
          emoji))
    for address in txinfo["destination"].keys():
      details = utils.search_db(self.conn, 'SELECT names, balance, hashtags, status FROM btcaddresses WHERE address="%s" AND dotweet=1' % (address))
      if details and len(details) and details[0] and len(details[0]):
        names, balance, hashtags, status = details[0][0], details[0][1], details[0][2], details[0][3]
        self.update_balance(address)
        balance = balance*self.config["exchangerates"]["btc2usd"] if balance >= 0 else -1
        for tag in self.config["ignorehashtags"].split("|"):
          hashtags = hashtags.replace(tag, "")
        hashtags = hashtags.strip("|")
        receiver = None
        if hashtags and hashtags != "":
          receiver = hashtags
        elif names and names != "":
          receiver = "https://blockchain.info/address/%s (%s)" % (address, names)
        else:
          receiver = "https://blockchain.info/address/%s" % (address)
        if status == 0:
          emoji = random.choice(self.config["neutralemojis"].split("|"))
        elif status == 1:
          emoji = random.choice(self.config["happyemojis"].split("|"))
        elif status == 2:
          emoji = random.choice(self.config["sademojis"].split("|"))
        else:
          emoji = random.choice(self.config["neutralemojis"])
        tweet.append("%s rcvd %f BTC ($%.2f) (https://blockchain.info/tx/%s) %s %s" % (
          receiver,
          txinfo["destination"][address],
          txinfo["destination"][address]*self.config["exchangerates"]["btc2usd"],
          txinfo["txhash"],
          #"(balance: %.02f) " % balance if balance >= 0 else "",
          self.config["generichashtags"],
          emoji))
    if tweet and len(tweet):
      tweet = " ".join(tweet)
      tweet = utils.unicodecp_to_unicodestr(tweet)
      utils.enqueue(queuefile=self.config["tweetqueue"], data=tweet)
      print("txparser:parser: %s" % (tweet))
      print("txparser:parser: added message to queue: %s (%d total)" % (self.config["tweetqueue"], utils.queuecount(queuefile=self.config["tweetqueue"])))

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    details = utils.search_db(self.conn, 'SELECT taskqueue, tweetqueue, generichashtags, ignorehashtags, happyemojis, neutralemojis, sademojis, queuemonitordelay from config')
    try:
      if details and details[0] and len(details[0]):
        self.config["taskqueue"], self.config["tweetqueue"], self.config["generichashtags"], self.config["ignorehashtags"], self.config["happyemojis"], self.config["neutralemojis"], self.config["sademojis"], self.config["queuemonitordelay"] = details[0][0], details[0][1], details[0][2], details[0][3], details[0][4], details[0][5], details[0][6], details[0][7]
      else:
        print("txparser:load_config: could not load config from database, using old config")
        self.config = copy.deepcopy(oldconfig)
    except:
      self.config = copy.deepcopy(oldconfig)

  def run(self):
    # load config from database
    self.load_config()

    # monitor txp queue
    print("txparser:run: monitoring queue: %s" % (self.config["taskqueue"]))
    count = utils.queuecount(queuefile=self.config["taskqueue"])
    while True:
      count = utils.queuecount(queuefile=self.config["taskqueue"])
      if count > 0:
        message = utils.dequeue(queuefile=self.config["taskqueue"])
        self.parser(message)
      time.sleep(self.config["queuemonitordelay"])

      # reload config from database
      self.load_config()
