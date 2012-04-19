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
from datetime import datetime, timedelta


#from eventlet import hubs
#hubs.use_hub("selects")
#hubs.use_hub("pyevent")

# Unbuffer outputs
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
TOTALIP = 3707764736

class InternetCrawler(object):
  def __init__(self, concurrency=12, fileForResults="results.json", ipI=0):
    # configuration 
    self.conf = {"timeoutConnect":5, "timeoutRequest":10}

    # a green pool is a pool of greenthreads - you're pushing
    # tasks to it and they get executed when eventlet's loop is
    # active
    self.pool = eventlet.GreenPool(concurrency + 5)
    # results
    self.results = eventlet.Queue()
    # iterator
    self.iIterator = internetIterator()
    # rewind the iterator to continue
    if ipI is not 0:
      print "Jump to the ip i: %d" % ipI
    
    for i in range(0,ipI):
      self.iIterator.next()

    print "Starting the crawler"

    # results file
    self.fileResults = open(fileForResults, "a+", 0)
    # stats
    self.stats = {"timeout":0,"connected":0,
          "success":0,"invalidHeaders":0,"requestTimeout":0,"connectError":0,
          "ip":"","ipI":0,"startedTime":""}
    self.stats['startedTime'] = datetime.now()

    print "Worker %d" % concurrency
    print "Write json to %s" % fileForResults

    for i in range(0,concurrency):
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
    match = re.compile("%s:\s*(.+?)\r\n"%header).search(body,re.IGNORECASE)
    if match == None:
      return None
    return match.groups()[0]

  def showStats(self):
    lastIpI = 0
    while True:
      print "Stats: %s" % strftime("%Y-%m-%d %H:%M:%S", gmtime())
      print "Progress: %f" % (float(self.stats['ipI']) / TOTALIP)
      perSecond = self.stats['ipI'] - lastIpI
      print "Per second: %d" % perSecond
      print "Estimated time left %s" % str(timedelta(seconds=TOTALIP / perSecond))
      print self.stats
      eventlet.sleep(1)

  def writer(self):
    while True:
      try:
        data = self.results.get()
        print data
        self.fileResults.write(json.dumps(data) + "\n")
        self.fileResults.flush()
      except:
        print >> sys.stderr, "Unexpected error ", sys.exc_info()[0]

  
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
    except:
      pass

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
          print >> sys.stderr, "Unexpected error ", sys.exc_info()[0]
          self.statsIncrement('errorUnexpected')
      
      if connected is None:
        continue

      with eventlet.timeout.Timeout(self.conf['timeoutRequest']):
        try:
          answer = self.request(sock)
          if len(answer) is 0:
            self.error(strIp,"No data received")
            self.statsIncrement('errorUnexpected')
            continue
          
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
          print "Success %s" % strIp
          self.statsIncrement('success')
        except eventlet.timeout.Timeout:
          self.error(strIp,"Request timeout")
          self.statsIncrement('requestTimeout')
        except:
          print >> sys.stderr, "Unexpected error ", sys.exc_info()[0]
          self.statsIncrement('errorUnexpected')


if __name__=="__main__":
  if len(sys.argv) is not 3:
    print "%s: <number of paralel workers> <skip to the iterator position>" % sys.argv[0]
    sys.exit()

  concurrency=int(sys.argv[1])
  startAtIpI=int(sys.argv[2])
  InternetCrawler(concurrency=concurrency,fileForResults="results.json",ipI=startAtIpI)
  print "End"