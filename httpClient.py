from __future__ import with_statement

from internetIterator import *

from eventlet.green import urllib2
from eventlet.green import socket
import json
import sys
import eventlet

class InternetCrawler(object):
  def __init__(self, concurrency=12, fileForResults="results.json"):
    # configuration 
    self.conf = {"timeoutSecond":5}

    # a green pool is a pool of greenthreads - you're pushing
    # tasks to it and they get executed when eventlet's loop is
    # active
    self.pool = eventlet.GreenPool(concurrency)

    # results
    self.results = eventlet.Queue()
    # iterator
    self.iIterator = internetIterator()
    # results file
    self.fileResults = open(fileForResults, 'w')
    # stats
    self.stats = {"timeout":0,"success":0,"ip":"","ipI":""}

    print "Worker %d" % concurrency
    print "Write json to %s" % fileForResults

    for i in range(0,concurrency - 2):
      print "Spawn %d" % i
      self.pool.spawn_n(self.worker)
    
    self.pool.spawn_n(self.writer)
    self.pool.spawn_n(self.showStats)

    self.pool.waitall()

  def showStats(self):
    print "Show stats"
    while True:
      print "Stats"
      print "Timeout: %d" % self.stats['timeout']
      print "Success: %d" % self.stats['success']
      print "Current ip(%d): %s" % (self.stats['ipI'],self.stats['ip'])
      eventlet.sleep(1)

  def writer(self):
    while True:
      data = self.results.get()
      self.fileResults.write(json.dumps(data) + "\n")
      print data

  def connect(self,ip,ipI):
      try:
        conn = socket.socket()

        conn.connect((str(ip), 80))
        print "Connected"

        conn.sendall('GET / HTTP/1.0\r\n\r\n')
        data = ""
        while True:
          recv = conn.recv(1024)
          if not recv: break
          data += recv
        server_info = {
          'index': ipI,  
          'ip': str(ip),
          'data': data
        }
        self.results.put(server_info)
        print "End"
      except socket.error, msg:
        print "Eroare %s" % msg

  def worker(self):
    while True:
      (ipI,ip) = self.iIterator.next()
      self.stats['ip'] = ip
      self.stats['ipI'] = ipI

      with eventlet.timeout.Timeout(self.conf['timeoutSecond']):
        try:
          self.connect(ip,ipI)
        except eventlet.timeout.Timeout:
          # counter of timeout requests
          self.stats['timeout'] = self.stats['timeout'] + 1

# fileForResults=sys.argv[1]
InternetCrawler(concurrency=10,fileForResults="results.json")
print "End"