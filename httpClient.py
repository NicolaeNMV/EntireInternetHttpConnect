from __future__ import with_statement

from internetIterator import *

from eventlet.green import urllib2
from eventlet.green import socket
import json
import sys
import eventlet
import re

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

    for i in range(0,concurrency - 5):
      print "Spawn %d" % i
      self.pool.spawn_n(self.worker)
    
    self.pool.spawn_n(self.writer)
    self.pool.spawn_n(self.showStats)
    self.pool.spawn_n(self.checkOurConnectivity)


    self.pool.waitall()

  # Make sure that we don't get shutdown
  def checkOurConnectivity(self):
    while True:
      conn = socket.socket()
      with eventlet.timeout.Timeout(self.conf['timeoutSecond']):
        try:
          print "Trying to connect to amazon"
          conn.connect(("aws.amazon.com", 80))
          conn.close()
        except eventlet.timeout.Timeout:
          print "Error connection! Cannot connect to my check server!"
      print "Connection ok"
      eventlet.sleep(60)
  
  #@staticmethod
  def getHTTPHeaders(self, body):
    match = re.compile("Server:(.+?)\r\n").search(body,re.IGNORECASE)
    if match == None:
      return False
    return match.groups()[0]

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
        data = conn.recv(1024)
        conn.close()
        
        #while True:
        #  recv = conn.recv(1024)
        #  if not recv: break
        #  data += recv
        server_info = {
          'index': ipI,  
          'ip': str(ip),
          'server': self.getHTTPHeaders(data)
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

if __name__=="__main__":
  # fileForResults=sys.argv[1]
  InternetCrawler(concurrency=10,fileForResults="results.json")
  print "End"