# -*- coding: utf-8 -*-

import jinja2

from pprint import pprint
import webbrowser
import subprocess
import random
import time
import copy

import utils

class statscollector():
  def __init__(self):
    super(statscollector, self).__init__()
    self.conn = utils.create_db("cryptopaymon.sqlite", "schema.sql")
    self.config = {
      "tweetqueue": None,
      "tweetmediaqueue": None,
      "generichashtags": None,
      "statscollectiondelay": 0,
      "satoshi2btc": 100000000,
      "templateopts": dict(),
      "basepath": "/media/shiv/red_third/stoolbox/gdrive-bckup/toolbox/cryptopaymon",
      "templatedir": "/media/shiv/red_third/stoolbox/gdrive-bckup/toolbox/cryptopaymon/html",
      "htmldir": "/media/shiv/red_third/stoolbox/gdrive-bckup/toolbox/cryptopaymon/html",
      "downloaddir": "/home/shiv",
      "exchangerates": {
        "btc2usd": 0
      },
      "imagesavedelay": 5,
      "heading_bad": "Statistics for Bitcoin Ransom",
      "heading_good": "Statistics for Bitcoin Donations",
    }

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    rows = utils.search_db(self.conn, 'SELECT tweetqueue, tweetmediaqueue, generichashtags, statscollectiondelay from config')
    try:
      if rows and rows[0] and len(rows[0]):
        self.config["tweetqueue"], self.config["tweetmediaqueue"], self.config["generichashtags"], self.config["statscollectiondelay"] = rows[0][0], rows[0][1], rows[0][2], rows[0][3]
      else:
        utils.info("could not load config from database")
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

  def address_summary(self):
    def write_file(filename, data):
      with open(filename, "w") as file:
        file.write(data)

    def entries_summary(rows, reporttype, filename, heading, timestamp):
      summary, hashtags, count = list(), list(), 0
      for row in rows:
        hashtags.append(row[0])
      hashtags = list(set(hashtags))
      for hashtag in sorted(hashtags):
        htrows = utils.search_db(self.conn, 'SELECT address, txs, rcvd, sent, balance, lasttx_epoch, lasttx_human FROM btcaddresses WHERE names="%s"' % (hashtag))
        if htrows and len(htrows):
          addresses, txs, rcvd, sent, balance, lasttx_epoch, lasttx_human = len(htrows), 0, 0, 0, 0, 0, None
          for htrow in htrows:
            if htrow[1] > 0:
              txs += htrow[1]
              rcvd += htrow[2]
              sent += htrow[3]
              balance += htrow[4]
              if lasttx_epoch < htrow[5]:
                lasttx_epoch = htrow[5]
                lasttx_human = htrow[6]
          if txs > 0:
            count += 1
            summary.append({
              "id": count,
              "wallet": hashtag,
              "addresses": addresses,
              "txs": txs,
              "rcvd": "%.2f" % (rcvd),
              "sent": "%.2f" % (sent),
              "balance": "%.2f" % (balance),
              "balanceusd": "%.2f" % (balance*self.config["exchangerates"]["btc2usd"]),
              "lasttx_human": lasttx_human,
            })
      # add this to html report template
      env = jinja2.Environment(loader=jinja2.FileSystemLoader(self.config["templatedir"]), **self.config["templateopts"])
      env.trim_blocks = True
      env.lsrtip_blocks = True
      write_file("%s/%s" % (self.config["htmldir"], filename), env.get_template("stats.template.html").render(entries=summary, reporttype=reporttype, heading=heading, timestamp=timestamp))
      # render html as an image and save to disk
      webbrowser.open_new("%s/%s" % (self.config["htmldir"], filename))
      time.sleep(self.config["imagesavedelay"])
      # read image data, add to tweetmedia queue, delete image file
      try:
        with open("%s/stats.png" % (self.config["downloaddir"]), "rb") as fo:
          imgdata = fo.read()
        utils.enqueue(queuefile=self.config["tweetmediaqueue"], data=imgdata)
        utils.info("added image data to queue")
      except:
        import traceback
        traceback.print_exc()
      utils.remove_file("%s/stats.png" % (self.config["downloaddir"]))

    timestamp = "%s UTC" % (utils.current_datetime_utc_string())
    rows = utils.search_db(self.conn, 'SELECT names FROM btcaddresses WHERE dostats=1 AND status=1')
    if rows and len(rows):
      entries_summary(rows, "good", "stats-donation.html", self.config["heading_good"], timestamp)
    rows = utils.search_db(self.conn, 'SELECT names FROM btcaddresses WHERE dostats=1 AND status=2')
    if rows and len(rows):
      entries_summary(rows, "bad", "stats-ransom.html", self.config["heading_bad"], timestamp)

  def run(self):
    # load config from database
    self.load_config()

    utils.info("starting statscollector module")
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
      utils.info("updated forex rates")
      # update btc address balance
      rows = utils.search_db(self.conn, 'SELECT address FROM btcaddresses')
      if rows and len(rows):
        count = 0
        for entry in rows:
          if self.update_balance(entry[0]):
            count += 1
        utils.info("updated balance for %d tracked addresses" % (count))

      # load exchange rates from stats table
      query = 'SELECT btc2usd FROM forex ORDER BY fid DESC LIMIT 1'
      rows = utils.search_db(self.conn, query)
      self.config["exchangerates"]["btc2usd"] = rows[0][0]

      # summary of all addresses
      summary = self.address_summary()

      # lowest|highest balance for ransom/donation recipients
      # most common sender/receiver
      # most common sender/receiver for ransom/donation
      # lowest|highest paying|receiving sender/receiver
      # highest balance/txs/rcvd/sent

      ## sleep
      time.sleep(self.config["statscollectiondelay"])

      # reload config from database
      self.load_config()


if __name__ == "__main__":
  statscollector().run()
