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

import sys, getopt, pickle, time, threading, xmlrpclib, logging
from datetime import datetime, timedelta
from xmlrpclib import Binary
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from sys import argv, exit
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from errno import ENOENT, ENOTEMPTY
from collections import defaultdict
MaxBLOCKSIZE=4
class Memory(LoggingMixIn, Operations):
  """Implements a hierarchical file system by using FUSE virtual filesystem.
     The file structure and data are stored in local memory in variable.
     Data is lost when the filesystem is unmounted"""

  def __init__(self,ms_helper, ds_helpers):
      self.ms_helper = ms_helper
      self.ds_helpers = ds_helpers

  def chmod(self, path, mode):
      if(self.ms_helper.chmod(Binary(path),Binary(mode) == 0)):
        return 0

  def chown(self, path, uid, gid):
      self.ms_helper.chown(Binary(path), Binary(uid) , Binary(gid))

  def create(self, path, mode):
      
      return self.ms_helper.create(Binary(path), Binary(str(mode)))

  def getattr(self, path, fh = None):

      attr_dict = pickle.loads(self.ms_helper.getattr(Binary(path)))
      
      if(attr_dict == -1):
        raise FuseOSError(ENOENT)
      else:
        return attr_dict

  def getxattr(self, path, name, position=0):
      
      return pickle.loads(self.ms_helper.getxattr(Binary(path), Binary(name)))

  def listxattr(self, path):
      
      attrs_keys = pickle.loads(self.ms_helper.listxattr(Binary(path)))
      return attrs_keys

  def mkdir(self, path, mode):
      
      print(str(mode))
      self.ms_helper.mkdir(Binary(path), Binary(str(mode)))

  def open(self, path, flags):
      
      return pickle.loads(self.ms_helper.open(Binary(path), Binary(str(flags))))

  def read(self, path, size, offset, fh):
      print("read")
      blocks = pickle.loads(self.ms_helper.read(Binary(path), Binary(str(size)), Binary(str(offset))))
      print(blocks)
      data = ''
      numDServers = len(self.ds_helpers)
      hash_val = int(blocks[0][:len(blocks[0])-2])
      print hash_val
      i = 0
      for b in blocks:
        server_id = (hash_val+i)%numDServers
        s = self.ds_helpers[server_id].get(Binary(str(b)))
        dat = pickle.loads(s)
        data = data + dat
        i=i+1

      print(data)
      return data[offset:]
      #   d = self.traverse(path, True)
      #   #case: offset > filesize
      #   if((offset//MaxBLOCKSIZE + 1) > len(d)):
      #     return ''

      #   #get first block where offset lies
      #   b_no = offset // MaxBLOCKSIZE
      #   data = ""
      #   data = d[b_no]
      #   # Case 1 : data is within one block
      #   if(size < MaxBLOCKSIZE):
      #     return data[offset%MaxBLOCKSIZE : offset%MaxBLOCKSIZE + size]

      #   # Case 2 : data spans over more than 1 block
      #   else:
      #     data = data[offset%MaxBLOCKSIZE :]
      #     no_of_blocks = len(d)
      #     if(b_no+size//MaxBLOCKSIZE < len(d)):
      #       no_of_blocks = b_no+size//MaxBLOCKSIZE
      #     for i in range(b_no+1, no_of_blocks):
      #       data += d[i]
      #     if(size - len(data) > 0 and no_of_blocks < len(d)):
      #       data+= d[(offset+size)//MaxBLOCKSIZE][:size - len(data)]
      #     return data
      # ##
      # #data = ''.join(d)
      #       #return data[offset:offset + size]

  def readdir(self, path, fh):

      return pickle.loads(self.ms_helper.readdir(Binary(path), fh))

  def readlink(self, path):
      
      return self.ms_helper.readlink(Binary(path)).data

  def removexattr(self, path, name):
      
      if(self.ms_helper.removexattr(Binary(path), Binary(name)).data == '1'):
        raise FuseOSError(ENOATTR)

  def rename(self, old, new):
      # po, po1 = self.traverseparent(old)
      # pn, pn1 = self.traverseparent(new)
      # if po['files'][po1]['st_mode'] & 0o770000 == S_IFDIR:
      #     po['st_nlink'] -= 1
      #     pn['st_nlink'] += 1
      # pn['files'][pn1] = po['files'].pop(po1)
      # do, do1 = self.traverseparent(old, True)
      # dn, dn1 = self.traverseparent(new, True)
      # dn[dn1] = do.pop(do1)
      print("rename")

  def rmdir(self, path):
      # p, tar = self.traverseparent(path)
      # if len(p['files'][tar]['files']) > 0:
      #     raise FuseOSError(ENOTEMPTY)
      # p['files'].pop(tar)
      # p['st_nlink'] -= 1
      print("rmdir")

  def setxattr(self, path, name, value, options, position=0):
      # Ignore options
      p = self.traverse(path.data)
      attrs = p.setdefault('attrs', {})
      attrs[name] = value
      

  def statfs(self, path):
      #return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
      print("statfs")

  def symlink(self, target, source):
      
      self.ms_helper.symlink(Binary(target), Binary(source))

  def truncate(self, path, length, fh = None):
      # print("*** length = ", length)
      # #print("file data = ",d)
      # d,d1 = self.traverseparent(path, True)
      # no_of_blocks = (length // MaxBLOCKSIZE) + 1
      # d[d1] = d[d1][:no_of_blocks]
      # d[d1][-1] = d[d1][-1][:length % MaxBLOCKSIZE]
      # p = self.traverse(path)
      # p['st_size'] = length
      print("truncate")

  def unlink(self, path):
      # p, tar = self.traverseparent(path)
      # p['files'].pop(tar)
      # d, d1 = self.traverseparent(path, True)
      # d[d1] = ['']
      print("unlink")

  def utimens(self, path, times = None):
      
      #print("utimes")
      self.ms_helper.utimens(Binary(path), times)

  def write(self, path, data, offset, fh):
      print("inside write")
      blockIDs = pickle.loads(self.ms_helper.write(Binary(path), Binary(data), Binary(str(offset))))
      print(blockIDs)
      print(data)
      print(str(offset))
      numDServers = len(self.ds_helpers)
      hash_val = int(blockIDs[0][:len(blockIDs[0])-2])
      print hash_val
      skip_blocks = offset//MaxBLOCKSIZE

      #write data to blocks choosing servers in round robin fashion
      for i in range(0, offset%MaxBLOCKSIZE):
        server_id = (hash_val + i)%numDServers
        self.ds_helpers[server_id].put(Binary(str(blockIDs[i])), Binary(""), Binary(str(MaxBLOCKSIZE)))

      k=0
      for i in range(offset//MaxBLOCKSIZE, (offset+len(data))//MaxBLOCKSIZE):
        server_id = (hash_val + i)%numDServers
        self.ds_helpers[server_id].put(Binary(str(blockIDs[i])), Binary(data[k*MaxBLOCKSIZE:(k+1)*MaxBLOCKSIZE]), Binary(str(0)))
        k=i+1

      if(len(blockIDs) >= k):
        server_id = (hash_val + k)%numDServers
        edge_block_data = self.ds_helpers[server_id].get(Binary(str(blockIDs[k])))
        self.ds_helpers[server_id].put(Binary(str(blockIDs[k])), Binary(data[k*MaxBLOCKSIZE:]),Binary(str(0)))
        if(len(edge_block_data) > len(data)%MaxBLOCKSIZE):
          self.ds_helpers[server_id].put(Binary(str(blockIDs[k])), Binary(edge_block_data[len(data)%MaxBLOCKSIZE:]), Binary(str(len(data)%MaxBLOCKSIZE)))

      if(len(blockIDs) > k):
        for i in range(k+1, len(blockIDs)):
          server_id = (hash_val+i)%numDServers
          self.ds_helpers[server_id].put(Binary(str(blockIDs[i])), Binary(""), Binary(str(MaxBLOCKSIZE)))

      return len(data)


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

  if len(argv) < 4:
        print('usage: %s <mountpoint> <metaserver> <dataserver1> .. <dataserverN>' % argv[0])
        exit(1)
  
  #get port number from options else use default
  server_url = "http://127.0.0.1:" + str(argv[2])
  #ms_helper = Helper(xmlrpclib.Server(server_url))
  ms_helper = xmlrpclib.Server(server_url)
  ds_helper = []
  for ds in argv[3:]:
    server_url = "http://127.0.0.1:" + str(ds)
    ds_helper.append(xmlrpclib.Server(server_url))
    #ds_helper.append(Helper(xmlrpclib.Server(server_url)))
    
  logging.basicConfig(level=logging.DEBUG)
  fuse = FUSE(Memory(ms_helper,ds_helper), argv[1], foreground=True, debug=True)


if __name__ == "__main__":
  main()