# -*- coding: utf-8 -*-

import websocket
import copy
import ast

from pprint import pprint
import json
import sys

import utils

class addressmonitor(object):
  def __init__(self):
    super(addressmonitor, self).__init__()
    self.conn = utils.create_db("cryptopaymon.sqlite", "schema.sql")
    self.config = {
      "taskqueue": None
    }
    self.ws = None
    self.addresses = None

    # https://blockchain.info/api/api_websocket
    self.wssurl = "wss://ws.blockchain.info/inv"
    self.ops = {
      "keepalive": '{"op":"ping"}',
      "unconfirmedtx": '{"op":"unconfirmed_sub"}',
      "subscribeaddr": '{"op":"addr_sub", "addr":"ADDRESS"}',
      "unsubscribeaddr": '{"op":"addr_unsub", "addr":"ADDRESS"}',
      "subscribeblock": '{"op":"blocks_sub"}',
      "lastblock": '{"op":"ping_block"}',
      "lasttx": '{"op":"ping_tx"}',
    }

  def on_open(self, ws):
    utils.info("addressmonitor:on_open: sending keepalive")
    ws.send(self.ops["keepalive"])
    self.subscribe()

  def on_message(self, ws, message):
    msgdict = json.loads(message)
    if msgdict["op"] == "utx" and "x" in msgdict:
      utils.enqueue(queuefile=self.config["taskqueue"], data=msgdict)
      utils.info("added message (%dB) to queue: %s (%d total)" % (len(message), self.config["taskqueue"], utils.queuecount(queuefile=self.config["taskqueue"])))
    # hack to ensure database changes are tracked without restarting module
    self.subscribe()

  def on_error(self, ws, error):
    utils.info("%s" % (error))

  def on_close(self, ws):
    utils.info("%s" % (ws.url))
    sys.exit(1)

  def subscribe(self):
    # get address, names from btcaddresses table
    rows = utils.search_db(self.conn, 'SELECT address, domonitor FROM btcaddresses')
    if rows and len(rows):
      subcount, unsubcount, self.addresses = 0, 0, []
      for entry in rows:
        # entry[1] is domonitor flag, if 0 unsubscribe, else subscribe
        if entry[1] == 0:
          self.ws.send(self.ops["unsubscribeaddr"].replace("ADDRESS", entry[0]))
          unsubcount += 1
        elif entry[1] == 1:
          if entry[0] not in self.addresses:
            self.addresses.append(entry[0])
          self.ws.send(self.ops["subscribeaddr"].replace("ADDRESS", entry[0]))
          subcount += 1
      utils.info("subscribed: %d | unsubscribed: %d" % (subcount, unsubcount))

  def enable_webscoket(self):
    websocket.enableTrace(False)
    self.ws = websocket.WebSocketApp(
      self.wssurl,
      on_message = self.on_message,
      on_error = self.on_error,
      on_close = self.on_close)
    self.ws.on_open = self.on_open
    self.ws.run_forever()

  def load_config(self):
    oldconfig = copy.deepcopy(self.config)
    rows = utils.search_db(self.conn, 'SELECT taskqueue FROM config')
    try:
      if rows and rows[0] and len(rows[0]):
        self.config["taskqueue"] = rows[0][0]
      else:
        utils.info("could not load config from database")
        self.config = copy.deepcopy(oldconfig)
    except:
      self.config = copy.deepcopy(oldconfig)

  def run(self):
    # load config from database
    self.load_config()

    try:
      # enable websocket and start monitoring
      utils.info("starting addressmonitor module")
      self.enable_webscoket()
    except:
      pass


if __name__ == "__main__":
  addressmonitor().run()
