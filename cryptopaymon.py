#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import multiprocessing

import populatetxs
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
    if not self.conn:
      self.conn = utils.create_db(self.config["dbfile"], self.config["schemafile"])

    try:
      pt = multiprocessing.Process(target=populatetxs.populatetxs().run)

      txp = multiprocessing.Process(target=txparser.txparser().run)
      two = multiprocessing.Process(target=tweetout.tweetout().run)
      ch = multiprocessing.Process(target=commandhandler.commandhandler().run)

      twi = multiprocessing.Process(target=tweetin.tweetin().run)
      sc = multiprocessing.Process(target=statscollector.statscollector().run)
      am = multiprocessing.Process(target=addressmonitor.addressmonitor().run)

      # these modules are independent of others
      # update all txs in case we miss any
      pt.start()

      # provide features like tweet replies and statscollection
      sc.start() ; two.start()

      # these modules monitor and process txs
      txp.start() ; am.start()

      # these modules provide cnc feature
      ch.start() ; twi.start()

      pt.join()
      sc.join() ; two.join()
      txp.join() ; am.join()
      ch.join() ; twi.join()
    except:
      import traceback
      traceback.print_exc()

if __name__ == "__main__":
  cpm = cryptopaymon()
  cpm.start()
