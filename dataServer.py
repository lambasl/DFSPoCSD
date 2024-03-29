#!/usr/bin/env python
#@authors: Amardeep Singh and Satbeer Lamba

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
import pickle
MaxBLOCKSIZE=512

def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = 51234
  if "--port" in ol:
    port = int(ol["--port"])
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  serve(port)

class DataServerHT:

  '''
  Data Server Hash Table. Stores key(Block ID) and the data in block as value. 
  It allows two RPC methods, get(blockID) to fetch the block and put(blockID, data)
  to write a block into Hash Table.
  '''
  def __init__(self):
    self.data = {}

  def get(self, key):
    '''

    '''
    k = str(key.data)
    print("request received for blockID:" + k)
    if k in self.data:
      print("data found..:" + self.data[k])
      return pickle.dumps(self.data[k])
    else:
      #raise ValueError("the block with key:" + str(key) + "was not found in Data Server")
      print("the block with key:" + str(key.data) + "was not found in Data Server")
      return pickle.dumps("No Val")

  def put(self, key, value, offset):
    '''
    puts the data in key block from the offset value. If data existed previously in the block same data is kept
    upto offset and beyond (offset+len(data)) and overwitten between offset and offset +len(data).
    If data does not exist upto offset in block, its filled with null chars upto offset.
    '''
    print('key:' + key.data)
    print('value:' + value.data)
    print('offset:' + str(offset))
    val = self.data[key.data] if key.data in self.data else ""
    print('old data' + val )
    offset = int(offset.data)
    if(offset <= len(val)):
      print('here 0')
      self.data[key.data] = val[:offset] + value.data
      print(self.data[key.data])
      if(len(val) > offset + len(value.data)):
        print('here 1')
        #hold on there is still more data to be copied to the block
        self.data[key.data] = self.data[key.data] + val[offset + len(value.data):]
        print(self.data[key.data])
    else:
      #time to add some nulls
      print('here3')
      null_chars = "\x00"*(offset - len(val))
      self.data[key.data] = val + null_chars + value.data
    print(self.data)

  def truncate(self, key, offset):
    offset = int(offset.data)
    if(offset != MaxBLOCKSIZE):
      if key.data not in self.data:
        raise ValueError("key does not exists")
      else:
        val = self.data[key.data]
        self.data[key.data] = val[:offset]

  def delete(self, key):
    if key.data in self.data:
      del self.data[key.data]

def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),allow_none = True)
  file_server.register_introspection_functions()
  sht = DataServerHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.truncate)
  file_server.register_function(sht.delete)

  print("Data Server Running")
  file_server.serve_forever()



class DataServerTest(unittest.TestCase):
  def test(self):
  	dataServerHT = DataServerHT()
  	self.assertEqual(dataServerHT.get("12345"), "hello", "No value for key=12345 empty")
  	self.assertEqual(dataServerHT.put("12345", "hello"), True, "key inserted")


if __name__ == "__main__":
    main()