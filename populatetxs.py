# -*- coding: utf-8 -*-

from pprint import pprint
import requests
import time
import copy
import re

import utils

import urllib3
urllib3.disable_warnings()

class populatetxs():
  def __init__(self):
    super(populatetxs, self).__init__()
    self.conn = utils.create_db("cryptopaymon.sqlite", "schema.sql")
    self.config = {
      "txupdatedelay": 0,
      "satoshi2btc": 1e8,
    }

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    rows = utils.search_db(self.conn, 'SELECT txupdatedelay FROM config')
    try:
      if rows and rows[0] and len(rows[0]):
        self.config["txupdatedelay"] = rows[0][0]
      else:
        utils.info("populatetxs:load_config: could not load config from database")
        self.config = copy.deepcopy(oldconfig)
    except:
      self.config = copy.deepcopy(oldconfig)

  def get_txs(self, address):
    customheaders = {
      "User-Agent": "Some script trying to be nice :)"
    }
    try:
      res = requests.get("https://blockchain.info/address/%s?format=json" % address, headers=customheaders, verify=False)
      if res.status_code == 200:
        return res.json()
      return None
    except:
      return None

  def get_tags(self, address):
    customheaders = {
      "User-Agent": "Some script trying to be nice :)"
    }
    try:
      tags = list()
      for tagid in [2, 4, 8, 16]:
        res = requests.get("https://blockchain.info/tags?address-filter=%s&filter=%d" % (address, tagid), headers=customheaders, verify=False)
        if res.status_code == 200:
          if 'span class="tag"' in res.text:
            match = re.search(r'span class="tag" id="([^"]+)">([^<]+)', res.text)
            if match and match.groups():
              tags.append(match.groups()[1])
      return tags
    except Exception as ex:
      import traceback
      traceback.print_exc()
      return None

  def update_database(self, rows):
    for row in rows:
      # get blockchain.info tags for current address
      tags = self.get_tags(row[0])

      # get list of all txs for current address
      txs = self.get_txs(row[0])
      if txs:
        if tags and len(tags):
          rcvd = txs["total_received"]/self.config["satoshi2btc"]
          sent = txs["total_sent"]/self.config["satoshi2btc"]
          balance = txs["final_balance"]/self.config["satoshi2btc"]
          query = 'UPDATE btcaddresses SET tags="%s", txs=%d, rcvd=%f, sent=%f, balance=%f WHERE address="%s"' % ("|".join(tags), txs["n_tx"], rcvd, sent, balance, row[0])
        else:
          rcvd = txs["total_received"]/self.config["satoshi2btc"]
          sent = txs["total_sent"]/self.config["satoshi2btc"]
          balance = txs["final_balance"]/self.config["satoshi2btc"]
          query = 'UPDATE btcaddresses SET txs=%d, rcvd=%f, sent=%f, balance=%f WHERE address="%s"' % (txs["n_tx"], rcvd, sent, balance, row[0])
        utils.populate_db(self.conn, query)
        txcount = 0
        for tx in txs["txs"]:
          # dict to store tx information
          txinfo = {
            "source": {},
            "destination": {},
            "txhash": tx["hash"],
            "timestamp_epoch": tx["time"],
            "timestamp_human": utils.epoch_to_human_utc(tx["time"]),
            "relayip": tx["relayed_by"],
            "rcvd": 0,
            "sent": 0,
          }

          # check if this tx is already in db
          if utils.search_db(self.conn, 'SELECT txhash FROM btctransactions WHERE txhash="%s"' % (tx["hash"])):
            continue

          # load source addresses and btc values
          for entry in tx["inputs"]:
            if entry.get("prev_out", None) and entry["prev_out"].get("addr", None) is not None and entry["prev_out"].get("addr", None) not in txinfo["source"]:
              txinfo["source"][entry["prev_out"]["addr"]] = entry["prev_out"]["value"]/self.config["satoshi2btc"]
              if utils.search_db(self.conn, 'SELECT address FROM btcaddresses WHERE address="%s"' % (entry["prev_out"]["addr"])):
                txinfo["sent"] = 1

          # load destination addresses and btc values
          for entry in tx["out"]:
            if entry.get("addr", None) is not None and entry["addr"] not in txinfo["destination"]:
              txinfo["destination"][entry["addr"]] = entry["value"]/self.config["satoshi2btc"]
              if utils.search_db(self.conn, 'SELECT address FROM btcaddresses WHERE address="%s"' % (entry["addr"])):
                txinfo["rcvd"] = 1

          # a tx with sent and rcvd both set has to be ignored, for now
          if txinfo["sent"] == 1 and txinfo["rcvd"] == 1:
            continue

          # update btcaddresses table with in|out address information
          alladdresses = utils.all_dict_keys([txinfo["source"], txinfo["destination"]])
          for address in alladdresses:
            query = 'SELECT inaddresses, outaddresses FROM btcaddresses WHERE address="%s"' % (address)
            rows = utils.search_db(self.conn, query)
            if rows and len(rows) and rows[0] and len(rows[0]):
              if rows[0][0]:
                inaddrs = "|".join(list(set(list(txinfo["source"].keys()) + [rows[0][0]])))
              else:
                inaddrs = "|".join(list(set(txinfo["source"].keys())))
              if rows[0][1]:
                outaddrs = "|".join(list(set(list(txinfo["destination"].keys()) + [rows[0][1]])))
              else:
                outaddrs = "|".join(list(set(txinfo["destination"].keys())))
                query = 'UPDATE btcaddresses SET inaddresses="%s", outaddresses="%s", lasttx_epoch="%s", lasttx_human="%s" WHERE address="%s"' % (inaddrs, outaddrs, txinfo["timestamp_epoch"], txinfo["timestamp_human"], address)
              utils.populate_db(self.conn, query)

          # update btctransactions table
          amountbtc, address = 0, None
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
          txcount += 1

        if txcount:
          utils.info("added %d new transactions for %s" % (txcount, row[2]))

  def update(self):
    statuses = [2, 1, 0]
    for status in statuses:
      rows = utils.search_db(self.conn, 'SELECT address, names, hashtags FROM btcaddresses WHERE status=%d' % (status))
      try:
        if rows and len(rows):
          self.update_database(rows)
      except:
        import traceback
        traceback.print_exc()
        pass

  def run(self):
    # load config from database
    self.load_config()

    utils.info("starting populatetxs module")
    while True:
      self.update()
      time.sleep(self.config["txupdatedelay"])
      self.load_config()


if __name__ == "__main__":
  populatetxs().run()
