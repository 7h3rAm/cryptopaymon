# -*- coding: utf-8 -*-

from pprint import pprint
import time
import copy

import utils

class statscollector():
  def __init__(self, conn):
    super(statscollector, self).__init__()
    self.conn = conn
    self.config = {
      "tweetqueue": None,
      "generichashtags": None,
      "statscollectiondelay": 0,
      "satoshi2btc": 100000000,
    }

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    details = utils.search_db(self.conn, 'SELECT tweetqueue, generichashtags, statscollectiondelay from config')
    try:
      if details and details[0] and len(details[0]):
        self.config["tweetqueue"], self.config["generichashtags"], self.config["statscollectiondelay"] = details[0][0], details[0][1], details[0][2]
      else:
        print("statscollector:load_config: could not load config from database")
        self.config = copy.deepcopy(oldconfig)
    except:
      self.config = copy.deepcopy(oldconfig)

  def update_balance(self, address):
    balance = utils.btc_balance(address)
    balance /= self.config["satoshi2btc"]
    if balance >= 0:
      query = 'UPDATE btcaddresses SET balance=%f WHERE address="%s"' % (balance, address)
      utils.populate_db(self.conn, query)
      return True
    return False

  def run(self):
    # load config from database
    self.load_config()

    print("statscollector:run: starting stats collection module")
    while True:
      # update exchange rates
      rates = utils.get_exchange_rates()
      if rates and len(rates):
        insertquery = 'INSERT INTO forex (btc2usd, btc2eur, btc2gbp, btc2cad, btc2sgd, btc2jpy, btc2inr) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s")' % (
          rates["USD"],
          rates["EUR"],
          rates["GBP"],
          rates["CAD"],
          rates["SGD"],
          rates["JPY"],
          rates["INR"],
        )
      deletequery = 'DELETE FROM forex WHERE fid IN (SELECT fid FROM forex LIMIT 1)'
      # add latest values
      utils.populate_db(self.conn, insertquery)
      # delete first row
      utils.populate_db(self.conn, deletequery)
      print("statscollector:run: updated forex rates")

      # update btc address balance
      # change this to domonitor to update balance for alltracked addresses
      details = utils.search_db(self.conn, 'SELECT address FROM btcaddresses WHERE dotweet=1')
      if details and len(details):
        count = 0
        for entry in details:
          if self.update_balance(entry[0]):
            count += 1
      print("statscollector:run: updated balance for %d tracked addresses" % (count))

      # lowest|highest balance for ransom recepients

      # lowest|highest balance for donation recepients

      # most common sender

      # most common receiver

      # most common receiver per sender

      # most common sender per receiver

      # sleep
      time.sleep(self.config["statscollectiondelay"])

      # reload config from database
      self.load_config()
