# -*- coding: utf-8 -*-

import sqlite3
import pika

from pprint import pprint
import functools
import datetime
import errno
import time
import sys
import re
import os

from _pickle import loads, dumps
from threading import get_ident
from time import sleep
import requests

import urllib3
urllib3.disable_warnings()

# http://flask.pocoo.org/snippets/88/
class squeue(object):
  _create = (
    'CREATE TABLE IF NOT EXISTS queue '
    '('
    '  id INTEGER PRIMARY KEY AUTOINCREMENT,'
    '  item BLOB'
    ')'
    )
  _count = 'SELECT COUNT(*) FROM queue'
  _iterate = 'SELECT id, item FROM queue'
  _append = 'INSERT INTO queue (item) VALUES (?)'
  _write_lock = 'BEGIN IMMEDIATE'
  _popleft_get = (
    'SELECT id, item FROM queue '
    'ORDER BY id LIMIT 1'
    )
  _popleft_del = 'DELETE FROM queue WHERE id = ?'
  _peek = (
    'SELECT item FROM queue '
    'ORDER BY id LIMIT 1'
    )

  def __init__(self, path):
    self.path = os.path.abspath(path)
    self._connection_cache = {}
    with self._get_conn() as conn:
      conn.execute(self._create)

  def __len__(self):
    with self._get_conn() as conn:
      #l = conn.execute(self._count).next()[0]
      l = conn.execute(self._count).fetchone()[0]
    return l

  def __iter__(self):
    with self._get_conn() as conn:
      for id, obj_buffer in conn.execute(self._iterate):
        yield loads(str(obj_buffer))

  def _get_conn(self):
    id = get_ident()
    if id not in self._connection_cache:
      self._connection_cache[id] = sqlite3.Connection(self.path, timeout=60)
    return self._connection_cache[id]

  def append(self, obj):
    obj_buffer = dumps(obj, 2)
    with self._get_conn() as conn:
      conn.execute(self._append, (obj_buffer,))

  def popleft(self, sleep_wait=True):
    keep_pooling = True
    wait = 0.1
    max_wait = 2
    tries = 0
    with self._get_conn() as conn:
      id = None
      while keep_pooling:
        conn.execute(self._write_lock)
        cursor = conn.execute(self._popleft_get)
        try:
          id, obj_buffer = cursor.fetchone()
          keep_pooling = False
        except StopIteration:
          conn.commit() # unlock the database
          if not sleep_wait:
            keep_pooling = False
            continue
          tries += 1
          sleep(wait)
          wait = min(max_wait, tries/10 + wait)
      if id:
        conn.execute(self._popleft_del, (id,))
        return loads(obj_buffer)
    return None

  def peek(self):
    with self._get_conn() as conn:
      cursor = conn.execute(self._peek)
      try:
        return loads(str(cursor.fetchone()[0]))
      except StopIteration:
        return None

def queuecount(queuefile):
  return squeue(queuefile).__len__()

def enqueue(queuefile, data):
  squeue(queuefile).append(data)

def dequeue(queuefile):
  return squeue(queuefile).popleft()

def create_db(dbfile, schemafile):
  if os.path.exists(dbfile):
    return sqlite3.connect(dbfile)
  else:
    with sqlite3.connect(dbfile) as conn:
      with open(schemafile, "rt") as f:
        schema = f.read()
      conn.executescript(schema)
      return conn

def populate_db(conn, query):
  try:
    with conn:
      cursor = conn.cursor()
      cursor.execute(query)
    return True
  except:
    return False

def search_db(conn, query):
  try:
    with conn:
      cursor = conn.cursor()
      cursor.execute(query)
      return cursor.fetchall()
  except sqlite3.OperationalError:
    return None
  except:
    return None

def btc_balance(address):
  # https://blockchain.info/balance?active=$address
  # always ensure to return -1 on error to differentiate between 0 balance n errors
  customheaders = {
    "User-Agent": "Some script trying to be nice :)"
  }
  try:
    res = requests.get("https://blockchain.info/balance?active=%s" % (address), headers=customheaders, verify=False)
    if res.status_code == 200:
      reply = res.json()
      if reply and address in reply and reply[address] and "final_balance" in reply[address] and reply[address]["final_balance"] >= 0:
        return reply[address]["final_balance"]
    return -1
  except Exception as ex:
    return -1

def get_exchange_rates():
  # https://blockchain.info/ticker
  customheaders = {
    "User-Agent": "Some script trying to be nice :)"
  }
  try:
    res = requests.get("https://blockchain.info/ticker", headers=customheaders, verify=False)
    if res.status_code == 200:
      reply = res.json()
      result = {}
      for curr in reply.keys():
        result[curr] = reply[curr]["last"]
      if result and len(result.keys()):
        return result
    return None
  except Exception as ex:
    return None

def epoch_to_human_localtime(epoch):
  return time.strftime("%d/%b/%Y %H:%M:%S %Z", time.localtime(epoch))

def epoch_to_human_utc(epoch):
  return time.strftime("%d/%b/%Y %H:%M:%S UTC", time.gmtime(epoch))

def all_dict_keys(listofdicts):
  return functools.reduce(set.union, map(set, map(dict.keys, listofdicts)))

def list_common(lista, listb):
  return list(set(lista) & set(listb))

def list_uncommon(lista, listb):
  return list(set(lista) ^ set(listb))

def remove_file(filename):
  # https://stackoverflow.com/a/10840586/1079836
  try:
    os.remove(filename)
  except OSError as e: # this would be "except OSError, e:" before Python 2.6
    if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
      raise # re-raise exception if a different error occurred

def unicodecp_to_unicodestr(str):
  # https://stackoverflow.com/a/41597732/1079836
  return re.sub(r'U\+([0-9a-fA-F]+)', lambda m: chr(int(m.group(1),16)), str)

def current_datetime_utc_string():
  return datetime.datetime.utcnow().strftime("%H:%M:%S %d/%b/%Y")

def current_datetime_string():
  return "%s %s" % (datetime.datetime.now().strftime("%H:%M:%S %d/%b/%Y"), time.tzname[0])

def exit(retcode=0):
  sys.exit(retcode)

# print message with debug level and function/module name
def doprint(msg, level="INFO", back=0):
  frame = sys._getframe(back + 1)
  filename = os.path.basename(frame.f_code.co_filename).replace(".py", "")
  funcname = frame.f_code.co_name
  lineno = frame.f_lineno
  print("%s [%s.%s.%d] %s: %s" % (current_datetime_string(), filename, funcname, lineno, level, msg))

# print info messages
def info(msg):
  pretext = "INFO"
  doprint(msg, pretext, back=1)

# print debug messages
def debug(msg):
  pretext = "DEBUG"
  doprint(msg, pretext, back=1)

# print warning messages
def warn(msg):
  pretext = "WARN"
  doprint(msg, pretext, back=1)

# print error messages
def error(msg):
  pretext = "ERROR"
  doprint(msg, pretext, back=1)
  exit(1)
