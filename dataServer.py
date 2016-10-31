

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary

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
    if key in self.data:
      self.data[key]
    else:
      #raise ValueError("the block with key:" + str(key) + "was not found in Data Server")
      print("the block with key:" + str(key) + "was not found in Data Server")
      return {}

  def put(self, key, value):
    self.data[key] = value;
    return True

def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = DataServerHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  print("Data Server Running")
  file_server.serve_forever()



class DataServerTest(unittest.TestCase):
  def test(self):
  	dataServerHT = DataServerHT()
  	self.assertEqual(dataServerHT.get("12345"), "hello", "No value for key=12345 empty")
  	self.assertEqual(dataServerHT.put("12345", "hello"), True, "key inserted")


if __name__ == "__main__":
    main()