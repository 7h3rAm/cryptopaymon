#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import multiprocessing

import addressmonitor
import statscollector
import commandhandler
import txparser
import tweetin
import tweetout
import utils

class cryptopaymon(object):
  def __init__(self):
    super(cryptopaymon, self).__init__()
    # load config from conf file into dict
    self.config = {
      "dbfile": "cryptopaymon.sqlite",
      "schemafile": "schema.sql",
    }
    self.conn = None

  def start(self):
    # upsert db
    if not self.conn:
      self.conn = utils.create_db(self.config["dbfile"], self.config["schemafile"])

    try:
      sc = multiprocessing.Process(target=statscollector.statscollector(self.conn).run)
      two = multiprocessing.Process(target=tweetout.tweetout(self.conn).run)

      txp = multiprocessing.Process(target=txparser.txparser(self.conn).run)
      am = multiprocessing.Process(target=addressmonitor.addressmonitor(self.conn).run)

      ch = multiprocessing.Process(target=commandhandler.commandhandler(self.conn).run)
      twi = multiprocessing.Process(target=tweetin.tweetin(self.conn).run)

      # these modules are independent of others
      # provide features like tweet replies and statscollection
      sc.start() ; two.start()

      # these modules monitor and process txs
      txp.start() ; am.start()

      # these modules provide cnc feature
      ch.start() ; twi.start()

      sc.join() ; two.join()
      txp.join() ; am.join()
      ch.join() ; twi.join()
    except:
      pass

if __name__ == "__main__":
  rm = cryptopaymon()
  rm.start()
