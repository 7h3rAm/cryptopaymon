# -*- coding: utf-8 -*-

from pprint import pprint
import random
import time
import copy

import utils

class dbupdate(object):
  def __init__(self):
    super(dbupdate, self).__init__()
    self.conn = utils.create_db("cryptopaymon.sqlite", "schema.sql")
    self.config = {}

  def update(self):
    rows = utils.search_db(self.conn, 'SELECT txhash, timestamp_human FROM btctransactions')
    if rows and len(rows):
      for row in rows:
        txhash, timestamp_human = row[0], row[1]
        if timestamp_human:
          print(1, txhash, timestamp_human, 1)
          #query = 'UPDATE btctransactions SET timestamp_human="%s" WHERE txhash="%s"' % (timestamp_human.replace("(UTC)", "UTC"), txhash)
          #utils.populate_db(self.conn, query)

  def run(self):
    self.update()


if __name__ == "__main__":
  dbupdate().run()
