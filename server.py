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

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib
from datetime import datetime, timedelta
from xmlrpclib import Binary
from time import time
from stat import S_IFDIR, S_IFLNK, S_IFREG

# Presents a HT interface
class SimpleHT:
  def __init__(self):
    self.files = {}
    self.dirs = {}
    now = time()
    self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                st_mtime=now, st_atime=now, st_nlink=2, files_name=[])


  def count(self):
    return len(self.files)

  # Retrieve something from the HT
  def get(self, key):
    # Default return value
    rv = {}
    # If the key is in the data structure, return properly formatted results
    key1 = key.data
    if key1 in self.files:
      rv = Binary(self.files[key1])
    return rv

  # Insert something into the HT
  # def put(self, key, value, ttl):
  #   # Remove expired entries
  #   metadata = value.data
  #   print(metadata)
  #   if('files_name' in metadata):
  #     self.dirs[key.data] = value.data
  #   else:
  #     self.files[key.data] = value.data
  #   path_split = key.data.split('/')
  #   parent = "".join(path_split[:-2])
  #   filename = path_split[-1]
  #   self.dirs[parent][files_name].append(filename)
  #   print(self.dirs)
  #   print(self.files)
  #   return True

  # Insert something into the HT
  def put(self, key, value, ttl):
    # Remove expired entries
    self.files[key.data] = value.data
    print(self.files)
    return True

  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.files = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.files, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.files
    return True

def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port="])
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = 51234
  if "--port" in ol:
    port = int(ol["--port"])
  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  print("Server Running")
  file_server.serve_forever()


if __name__ == "__main__":
  main()