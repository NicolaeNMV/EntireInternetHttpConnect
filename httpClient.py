from __future__ import with_statement

from internetIterator import *

from eventlet.green import urllib2
from eventlet.green import socket
import json
import sys
import os
import eventlet
import re
from time import gmtime, strftime


#from eventlet import hubs
#hubs.use_hub("selects")
#hubs.use_hub("pyevent")

# Unbuffer outputs
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

class InternetCrawler(object):
  def __init__(self, concurrency=12, fileForResults="results.json", ipI=0):
    # configuration 
    self.conf = {"timeoutConnect":5, "timeoutRequest":10}

    # a green pool is a pool of greenthreads - you're pushing
    # tasks to it and they get executed when eventlet's loop is
    # active
    self.pool = eventlet.GreenPool(concurrency)
    # results
    self.results = eventlet.Queue()
    # iterator
    self.iIterator = internetIterator()
    # rewind the iterator to continue
    for i in range(0,ipI):
      self.iIterator.next()


    # results file
    self.fileResults = open(fileForResults, 'w+', 0)
    # stats
    self.stats = {"timeout":0,"connected":0,
          "success":0,"invalidHeaders":0,"requestTimeout":0,"connectError":0,"ip":"","ipI":""}

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
      with eventlet.timeout.Timeout(self.conf['timeoutConnect']):
        try:
          conn.connect(("aws.amazon.com", 80))
          conn.close()
          print "Connectivity check: OK"
        except eventlet.timeout.Timeout:
          print "Connectivity check: FAIL! Error connection! Cannot connect to my check server!"
      eventlet.sleep(1)
  
  #@staticmethod
  def getHTTPHeader(self, body, header):
    match = re.compile("%s:(.+?)\r\n"%header).search(body,re.IGNORECASE)
    if match == None:
      return None
    return match.groups()[0]

  def showStats(self):
    while True:
      print "Stats: %s" % strftime("%Y-%m-%d %H:%M:%S", gmtime())
      print self.stats
      eventlet.sleep(1)

  def writer(self):
    while True:
      print "Write data"
      data = self.results.get()
      self.fileResults.write(json.dumps(data) + "\n")
      self.fileResults.flush()
      print data
  
  def request(self,conn):
    try:
      conn.sendall('GET / HTTP/1.0\r\n\r\n')

      data = ""
      while True:
        recv = conn.recv(1024)
        if not recv: break
        data += recv
        if len(data) > 200: break
      return data
    except socket.error, msg:
      print "Request error %s" % msg

  def statsIncrement(self,name):
    self.stats[name] += 1

  def error(self,ip,message):
    print >> sys.stderr, (ip,message)

  def worker(self):
    sock = None

    while True:
      if sock is not None:
        sock.close()

      sock = socket.socket()
      (ipI,ip) = self.iIterator.next()
      self.stats['ip'] = ip
      self.stats['ipI'] = ipI
      strIp = str(ip)

      connected = None
      with eventlet.timeout.Timeout(self.conf['timeoutConnect']):
        try:
          sock.connect((strIp, 80))
          self.statsIncrement('connected')
          connected = True
        except socket.error, msg:
          self.error(strIp,"Connect error %s" % msg)
          self.statsIncrement('connectError')
        
        except eventlet.timeout.Timeout:
          self.statsIncrement('timeout')
        
        except:
          print "Unexpected error:", sys.exc_info()[0]
          self.statsIncrement('errorUnexpected')
      
      if connected is None:
        continue

      with eventlet.timeout.Timeout(self.conf['timeoutRequest']):
        try:
          answer = self.request(sock)
          serverName = self.getHTTPHeader(answer,"Server")
          if serverName is None:
            self.error(strIp,"Invalid headers %s" % answer)
            self.statsIncrement('invalidHeaders')
            continue
          
          server_info = {
            'index': ipI,  
            'ip': strIp,
            'server': serverName
          }
          self.results.put(server_info)
          
          self.statsIncrement('success')
        except eventlet.timeout.Timeout:
          self.statsIncrement('requestTimeout')


if __name__=="__main__":
  # fileForResults=sys.argv[1]
  InternetCrawler(concurrency=10,fileForResults="results.json",ipI=1621243)
  print "End"