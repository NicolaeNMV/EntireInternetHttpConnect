from __future__ import with_statement

from internetIterator import *

from eventlet.green import urllib2
from eventlet.green import socket
import json
import sys
import eventlet

class InternetCrawler(object):
  def __init__(self, concurrency=10, fileForResults="results.json"):
    # a green pool is a pool of greenthreads - you're pushing
    # tasks to it and they get executed when eventlet's loop is
    # active
    self.pool = eventlet.GreenPool(concurrency)

    # Results
    self.results = eventlet.Queue()
    # counter of failed requests
    self.failed = 0
    # iterator
    self.iIterator = internetIterator()
    # Results file
    self.fileResults = open(fileForResults, 'w')

    print "Worker %d" % concurrency
    print "Write json to %s" % fileForResults

    for i in range(0,concurrency):
      print "Spawn %d" % i
      self.pool.spawn_n(self.worker)
    
    self.pool.spawn_n(self.writer)

    self.pool.waitall()

  def writer(self):
    while 1:
      data = self.results.get()
      self.fileResults.write(json.dumps(data) + "\n")
      print data

  def worker(self):
    print "Worker"
    (ipI,ip) = self.iIterator.next()

    conn = socket.socket()
    try:
      conn.connect((str(ip), 80))
      print '%s connected' % ip

      conn.sendall('GET / HTTP/1.0\r\n\r\n')
      data = ""
      while 1:
        recv = conn.recv(1024)
        if not recv: break
        data += recv
      server_info = {
        'index': ipI,  
        'ip': str(ip),
        'data': data
      }
      self.results.put(server_info)
    except socket.error, msg:
      print "Eroare %s" % msg
    return 

InternetCrawler(concurrency=10,fileForResults=sys.argv[1])
print "End"