#!/usr/bin/env python
#@authors: Amardeep Singh and Satbeer Lamba

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
import pickle
import shelve #1
import hashlib
import random
import os.path

MaxBLOCKSIZE=512

def main():
  dsList = sys.argv[2:]
  numDServers = len(dsList)
  port = dsList[int(sys.argv[1])]

  #print(dsList, port)
  serve(int(port), sys.argv[1:])


class DataServerHT:

  '''
  Data Server Hash Table. Stores key(Block ID) and the data in block as value. 
  It allows two RPC methods, get(blockID) to fetch the block and put(blockID, data)
  to write a block into Hash Table.
  '''
  def __init__(self, args):
    dsList = args[1:]
    numDServers = len(dsList)
    filename_this  = "dataServer"+str(sys.argv[1]) + "_own"
    #filename_prev  = "dataServer"+str(sys.argv[1]) + "_prev"
    prev_port = dsList[int(args[0]) -1]
    next_port = dsList[(int(args[0]) +1)%numDServers]
    print("prev and next ports:", prev_port, next_port)
    prev_DS = xmlrpclib.Server("http://127.0.0.1:" + str(prev_port))
    next_DS = xmlrpclib.Server("http://127.0.0.1:" + str(next_port))
    firstStart = False #wherther the server is being started first time
    try:
      next_DS.getDataBlocks()
      firstStart = False
    except Exception as e:
      print('error connecting to next data server:')
      print(e)
      firstStart = True
    print('first start:', firstStart)
    if(firstStart or os.path.exists(filename_this)):
      self.data = shelve.open(filename_this, writeback = True)
      if(firstStart):
        self.data['own']={}
        self.data['prev']={}
        self.data.sync()
      #self.prevData = shelve.open(filename_prev, writeback = True)
    else:    
      self.data = shelve.open(filename_this, writeback=True)
      self.data['own'] = pickle.loads(next_DS.getDataBlocks(False))
      self.data['prev'] = pickle.loads(prev_DS.getDataBlocks(True))
      self.data.sync()
      #self.prevData = pickle.loads(prev_DS.getDataBlocks(True))
      #self.prevData.sync()
    print(self.data)

  def getDataBlocks(self, getOwn=True):
    if(getOwn):
      return pickle.dumps(self.data['own'])
    else:
      return pickle.dumps(self.data['prev'])

  def get(self, key, isSec=False):
    '''

    '''
    print(self.data)
    if(isSec):
      return self.getInternal(self.data['prev'], key)
    else:
      return self.getInternal(self.data['own'], key)

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
      self.putInternal(self.data['prev'], key, value, offset)
    else:
      self.putInternal(self.data['own'], key, value, offset)
    self.data.sync()

  def corrupt(self, block_id):

    print(self.data)
    if block_id in self.data['own']:
      print("1")
      list_data = list(self.data['own'][block_id][0])
      random.shuffle(list_data)
      self.data['own'][block_id][0] = ''.join(list_data)
      print(self.data['own'][block_id][0])

    elif block_id in self.data['prev']:
      print("1")
      list_data = list(self.data['prev'][block_id][0])
      random.shuffle(list_data)
      self.data['prev'][block_id][0] = ''.join(list_data)
      print(self.data['prev'][block_id][0])
      #self.data[block_id][0] = str(int(self.data[block_id][0][0])-1) + self.data[block_id][0][1:]

  def putInternal(self, dataObj, key, value, offset):
    val = dataObj[key.data][0] if key.data in dataObj else ""
    offset = int(offset.data)
    if(offset <= len(val)):
      d = val[:offset] + value.data
      checksum = self.getCheckSum(d)
      dataObj[key.data] = [ d, checksum] 
      if(len(val) > offset + len(value.data)):
        #hold on there is still more data to be copied to the block
        d = dataObj[key.data][0] + val[offset + len(value.data):]
        checksum = self.getCheckSum(d)
        dataObj[key.data] = [ d, checksum]
    else:
      #time to add some nulls
      null_chars = "\x00"*(offset - len(val))
      d = val + null_chars + value.data
      checksum = self.getCheckSum(d)
      dataObj[key.data] = [ d, checksum]
    #dataObj.sync()

  def getCheckSum(self, d):
      ho = hashlib.md5(bytearray(d))
      return ho.hexdigest()

  def truncate(self, key, offset, isSec=False):
    '''
    Removes data from block key from offset.
    '''
    if(isSec):
      self.truncateInternal(self.data['prev'], key, offset)
    else:
      self.truncateInternal(self.data['own'], key, offset)
    self.data.sync()
  def truncateInternal(self, dataObj, key, offset):
    offset = int(offset.data)
    if(offset != MaxBLOCKSIZE):
      if key.data not in dataObj:
        raise ValueError("key does not exists")
      else:
        val = dataObj[key.data]
        dataObj[key.data] = val[:offset]
        #dataObj.sync()
        
  def delete(self, key, isSec=False):
    '''
    Deletes the block key from hash table
    '''
    if(isSec):
      self.deleteInternal(self.data['prev'], key)
    else:
      self.deleteInternal(self.data['own'], key)
    self.data.sync()

  def deleteInternal(self, dataObj, key):
    if key.data in dataObj:
      del dataObj[key.data]
      #dataObj.sync()

def serve(port, args):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),allow_none = True)
  file_server.register_introspection_functions()
  sht = DataServerHT(args)
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.truncate)
  file_server.register_function(sht.delete)
  file_server.register_function(sht.corrupt)
  file_server.register_function(sht.getDataBlocks)

  print("Data Server Running")
  file_server.serve_forever()



class DataServerTest(unittest.TestCase):
  def test(self):
  	dataServerHT = DataServerHT()
  	self.assertEqual(dataServerHT.get("12345"), "hello", "No value for key=12345 empty")
  	self.assertEqual(dataServerHT.put("12345", "hello"), True, "key inserted")


if __name__ == "__main__":
    main()