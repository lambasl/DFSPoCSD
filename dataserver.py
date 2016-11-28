#!/usr/bin/env python
#@authors: Amardeep Singh and Satbeer Lamba

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
import pickle
import shelve #1

MaxBLOCKSIZE=512

def main():
  dsList = sys.argv[2:]
  port = dsList[int(sys.argv[1])]
  #print(dsList, port)
  serve(int(port))


class DataServerHT:

  '''
  Data Server Hash Table. Stores key(Block ID) and the data in block as value. 
  It allows two RPC methods, get(blockID) to fetch the block and put(blockID, data)
  to write a block into Hash Table.
  '''
  def __init__(self):
    filename_this  = "dataServer"+str(sys.argv[1]) + "_own"
    filename_prev  = "dataServer"+str(sys.argv[1]) + "_prev"
    self.data = shelve.open(filename_this, writeback = True)
    self.prevData = shelve.open(filename_prev, writeback = True)
    #print(self.data)

  def get(self, key, isSec=False):
    '''

    '''
    if(isSec):
      return self.getInternal(self.prevData, key)
    else:
      return self.getInternal(self.data, key)

  def getInternal(self, dataObj, key):
    k = str(key.data)
    if k in dataObj:
      return pickle.dumps(dataObj[k])
    else:
      #raise ValueError("the block with key:" + str(key) + "was not found in Data Server")
      print("the block with key:" + str(key.data) + "was not found in Data Server")
      return pickle.dumps("No Val")

  def put(self, key, value, offset, isSec=False):
    '''
    puts the data in key block from the offset value. If data existed previously in the block same data is kept
    upto offset and beyond (offset+len(data)) and overwitten between offset and offset +len(data).
    If data does not exist upto offset in block, its filled with null chars upto offset.
    '''
    if(isSec):
      self.putInternal(self.prevData, key, value, offset)
    else:
      self.putInternal(self.data, key, value, offset)

  def putInternal(self, dataObj, key, value, offset):
    val = dataObj[key.data] if key.data in dataObj else ""
    offset = int(offset.data)
    if(offset <= len(val)):
      dataObj[key.data] = val[:offset] + value.data
      if(len(val) > offset + len(value.data)):
        #hold on there is still more data to be copied to the block
        dataObj[key.data] = dataObj[key.data] + val[offset + len(value.data):]
    else:
      #time to add some nulls
      null_chars = "\x00"*(offset - len(val))
      dataObj[key.data] = val + null_chars + value.data
    dataObj.sync()



  def truncate(self, key, offset, isSec=False):
    '''
    Removes data from block key from offset.
    '''
    if(isSec):
      self.truncateInternal(dataObj, key, offset)
    else:
      self.truncateInternal(dataObj, key, offset)

  def truncateInternal(self, dataObj, key, offset):
    offset = int(offset.data)
    if(offset != MaxBLOCKSIZE):
      if key.data not in dataObj:
        raise ValueError("key does not exists")
      else:
        val = dataObj[key.data]
        dataObj[key.data] = val[:offset]
        dataObj.sync()
        
  def delete(self, key, isSec=False):
    '''
    Deletes the block key from hash table
    '''
    if(isSec):
      self.deleteInternal(self.prevData, key)
    else:
      self.deleteInternal(self.data, key)

  def deleteInternal(self, dataObj, key):
    if key.data in dataObj:
      del dataObj[key.data]
      dataObj.sync()

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