#!/usr/bin/env python
#@authors: Amardeep Singh and Satbeer Lamba

import sys, getopt, pickle, time, threading, xmlrpclib, logging
from datetime import datetime, timedelta
from xmlrpclib import Binary
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from sys import argv, exit
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from errno import ENOENT, ENOTEMPTY
from collections import defaultdict
import hashlib
import socket
import time

MaxBLOCKSIZE=512

numDServers=0
class Memory(LoggingMixIn, Operations):
  """Implements a hierarchical file system by using FUSE virtual filesystem.
     The file structure and data are stored in local memory in variable.
     Data is lost when the filesystem is unmounted"""

  def __init__(self,ms_helper, ds_helpers):
      self.ms_helper = ms_helper
      self.ds_helpers = ds_helpers

  def chmod(self, path, mode):
      if(self.ms_helper.chmod(Binary(path),mode) == 0):
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
      
      #print(str(mode))
      self.ms_helper.mkdir(Binary(path), Binary(str(mode)))

  def open(self, path, flags):
      
      return pickle.loads(self.ms_helper.open(Binary(path), Binary(str(flags))))

  def getCheckSum(self, d):
      ho = hashlib.md5(bytearray(d))
      return ho.hexdigest()

  def verifyCheckSum(self, d):
      if(d[1] == self.getCheckSum(d[0])):
        return True
      else:
        return False

  def read(self, path, size, offset, fh):

      #list of corrupt blocks
      cpt_blks_s1 = []
      cpt_blks_s2 = []
      print("read")
      blocks = pickle.loads(self.ms_helper.read(Binary(path), Binary(str(size)), Binary(str(offset))))
      #print(blocks)
      if not blocks:
        return ""
      data = ''
      numDServers = len(self.ds_helpers)
      hash_val = int(pickle.loads(self.ms_helper.gethashVal(Binary(path))))
      #print hash_val
      i = 0
      d_2_append = ""
      for b in blocks:
        server_id = (hash_val+i)%numDServers
        try:
          s = self.ds_helpers[server_id].get(Binary(str(b)))
          dat_S1 = pickle.loads(s)
          if(self.verifyCheckSum(dat_S1)):
            d_2_append = dat_S1[0]
          else:
            cpt_blks_s1.append(str(b))
        except socket.error as err:
          print("Server with id {} is down!".format(server_id))
        

        try:
          s = self.ds_helpers[(server_id+1)%numDServers].get(Binary(str(b)), True)
          dat_S2 = pickle.loads(s)
          if(self.verifyCheckSum(dat_S2)):
            d_2_append = dat_S2[0]
          else:
            cpt_blks_s2.append(str(b))
        except socket.error as err:
          print("Server with id {} is down!".format((server_id+1)%numDServers))

        
        data = data + d_2_append
        i=i+1
      if(cpt_blks_s1):
        for blk in cpt_blks_s1:
          d_list = self.ds_helpers[(server_id+1)%numDServers].get(Binary(blk))
          dat_S2 = pickle.loads(d_list)
          self.ds_helpers[server_id].put(Binary(blk), Binary(dat_S2[0]), Binary(str(0)))

      if(cpt_blks_s2):
        for blk in cpt_blks_s1:
          d_list = self.ds_helpers[server_id].get(Binary(blk))
          dat_S1 = pickle.loads(d_list)
          self.ds_helpers[server_id].put(Binary(blk), Binary(dat_S1[0]), Binary(str(0)))

      
      #print(data)
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
      self.ms_helper.rename(Binary(old), Binary(new))

  def rmdir(self, path):
      if(self.ms_helper.rmdir(Binary(path)).data == '1'):
        raise FuseOSError(ENOTEMPTY)

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
      
      print("truncate")
      hash_val = pickle.loads(self.ms_helper.gethashVal(Binary(path)))
      delete_blocks = pickle.loads(self.ms_helper.truncate(Binary(path), Binary(str(length))))
      offset = length%MaxBLOCKSIZE
      for b in delete_blocks:
        block_num = int(b[len(hash_val)+1:])
        server_id = (int(hash_val) + block_num)%numDServers
        if(offset != 0):
          self.ds_helpers[server_id].truncate(Binary(b), Binary(offset))
          self.ds_helpers[(server_id+1)%numDServers].truncate(Binary(b), Binary(offset), True)
          offset=0
        else:
          self.ds_helpers[server_id].delete(Binary(b))
          self.ds_helpers[(server_id+1)%numDServers].delete(Binary(b), True)

  def unlink(self, path):
      
      hash_val = pickle.loads(self.ms_helper.gethashVal(Binary(path)))
      blocks = pickle.loads(self.ms_helper.unlink(Binary(path)))
      #perform unlink on data servers only if its not a symlink, as symlink has no data on data servers
      if(blocks != "symlink"):
        #print('hash', hash_val)
        for b in blocks:
          block_num = b[len(hash_val):]
          server_id = (int(hash_val) + int(block_num))%numDServers
          self.ds_helpers[server_id].delete(Binary(b))
          self.ds_helpers[(server_id+1)%numDServers].delete(Binary(b), True)
        #print('deleted blocks:', blocks)

  def utimens(self, path, times = None):
      
      #print("utimes")
      self.ms_helper.utimens(Binary(path), times)

  def write(self, path, data, offset, fh):
      blockIDs = pickle.loads(self.ms_helper.write(Binary(path), Binary(data), Binary(str(offset))))
      hash_val = int(pickle.loads(self.ms_helper.gethashVal(Binary(path))))
      skip_blocks = offset//MaxBLOCKSIZE
      #we are returning all the blocks here and skipping the ones we dont need to overwite
      #write data to blocks choosing servers in round robin fashion

      #if offset is 0 nothing happens else same data nulls are written to the blocks as per case
 
      for i in range(0, offset//MaxBLOCKSIZE):
        #print("moving few data blocks")
        server_id = (hash_val + i)%numDServers
        #print('server', server_id)
        self.blockPut(server_id, Binary(str(blockIDs[i])), Binary(""), Binary(str(MaxBLOCKSIZE)))
        self.blockPut((server_id+1)%numDServers, Binary(str(blockIDs[i])), Binary(""), Binary(str(MaxBLOCKSIZE)), True)
        #self.ds_helpers[server_id].put(Binary(str(blockIDs[i])), Binary(""), Binary(str(MaxBLOCKSIZE)))
        #self.ds_helpers[(server_id+1)%numDServers].put(Binary(str(blockIDs[i])), Binary(""), Binary(str(MaxBLOCKSIZE)), True)

      up = (offset+len(data))//MaxBLOCKSIZE if ((offset+len(data))%MaxBLOCKSIZE) == 0 else (offset+len(data))//MaxBLOCKSIZE + 1
      k=0
      first_offset = 0 if offset%MaxBLOCKSIZE == 0 else offset%MaxBLOCKSIZE
      start = 0
      end = MaxBLOCKSIZE
      #print("first offset:" + str(first_offset))
      for i in range(offset//MaxBLOCKSIZE, up):
        server_id = (hash_val + i)%numDServers
        #print('server', server_id)
        #print('iterator:' + str(i))
        if(first_offset == 0):
          #print("start:" + str(start) + ",end:" + str(end))
          self.blockPut(server_id, Binary(str(blockIDs[i])), Binary(data[start:end]), Binary(str(0)))
          self.blockPut((server_id+1)%numDServers, Binary(str(blockIDs[i])), Binary(data[start:end]), Binary(str(0)), True)
          #self.ds_helpers[server_id].put(Binary(str(blockIDs[i])), Binary(data[start:end]), Binary(str(0)))
          #self.ds_helpers[(server_id+1)%numDServers].put(Binary(str(blockIDs[i])), Binary(data[start:end]), Binary(str(0)), True)
        else:
          start = 0
          end = MaxBLOCKSIZE - first_offset
          #print("start:" + str(start) + ",end:" + str(end))
          self.blockPut(server_id, Binary(str(blockIDs[i])), Binary(data[start:end]), Binary(str(first_offset)))
          self.blockPut((server_id+1)%numDServers, Binary(str(blockIDs[i])), Binary(data[start:end]), Binary(str(first_offset)), True)
          #self.ds_helpers[server_id].put(Binary(str(blockIDs[i])), Binary(data[start:end]), Binary(str(first_offset)))
          #self.ds_helpers[(server_id+1)%numDServers].put(Binary(str(blockIDs[i])), Binary(data[start:end]), Binary(str(first_offset)), True)
          first_offset = 0
        start = end;
        end = start + MaxBLOCKSIZE
        k=k+1 
      return len(data)

  def blockPut(self, server_id, key, val, offset, isSec=False):
    flag = False
    while(not flag):
      try:
        self.ds_helpers[server_id].put(key, val, offset, isSec)
        flag = True
      except Exception as e:
        print('error while writing data to server:', server_id)
        print(e)
        time.sleep(5)
        flag = False

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
  global numDServers 
  numDServers = len(ds_helper)
  print("num of data servers", numDServers)
  logging.basicConfig(level=logging.DEBUG)
  fuse = FUSE(Memory(ms_helper,ds_helper), argv[1], foreground=True, debug=True)


if __name__ == "__main__":
  main()