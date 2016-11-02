#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.03

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => Binary
      print rv.data => "value"
  put(base64 key, base64 value)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"))
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable

Changelog:
    0.03 - Modified to remove timeout mechanism for data.
"""

import sys, getopt, pickle, time, threading, xmlrpclib
from datetime import datetime, timedelta
from xmlrpclib import Binary
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

MaxBLOCKSIZE=128

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

  def list_methods(self):
    return self.caller.system.listMethods()


  # Test via RPC
def main():
  
  #get port number from options else use default
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port="])
  ol={}

  for k,v in optlist:
    ol[k] = v
  port = 51234

  if "--port" in ol:
    port = int(ol["--port"])
  server_url = "http://127.0.0.1:" + str(port)

  helper = Helper(xmlrpclib.Server(server_url))
  #print(helper.list_methods())

  #Filesystem code

  data = {'a':'a', 'files_name':'d'}
 
  helper.put("/hello.c", pickle.dumps(data), 10000)
  if(pickle.loads(helper.get("/hello.c").data) == data):
    print("put was successsful")
  

if __name__ == "__main__":
  main()