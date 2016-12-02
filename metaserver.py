#!/usr/bin/env python
#@authors: Amardeep Singh and Satbeer Lamba

import logging

from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib
from datetime import datetime, timedelta
from xmlrpclib import Binary
from time import time
from stat import S_IFDIR, S_IFLNK, S_IFREG
import random
import hashlib

MaxBLOCKSIZE=512

# Presents a HT interface
class SimpleHT:
    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                st_mtime=now, st_atime=now, st_nlink=2, files={})
        # The key 'files' holds a dict of filenames(and their attributes
        #  and 'files' if it is a directory) under each level
    def gethashVal(self, bin_path):
        path = bin_path.data
        #print('path' , path)
        return pickle.dumps(self.traverse(path)['hash_val'])

    def hashVal(self, path):
        '''
        method to generate hash value on passing path
        '''
        return str(int(hashlib.md5(path).hexdigest()[:8],16))

    def traverse(self, path):
        """Traverses the dict of dict(self.files) to get pointer
            to the location of the current file."""
        p = self.files['/']    
        for i in path.split('/') :
            p = p['files'][i] if len(i) > 0 else p
        return p

    def traverseparent(self, path):
        """Traverses the dict of dict(self.files) to get pointer
            to the parent directory of the current file.
            Also returns the child name as string"""
        p = self.files['/']
        target = path[path.rfind('/')+1:]
        path = path[:path.rfind('/')]
        for i in path.split('/') :
                p = p['files'][i] if len(i) > 0 else p
        return p, target

    def chmod(self, path, mode):
        p = self.traverse(path.data)
        p['st_mode'] &= 0o770000
        p['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        p = self.traverse(path.data)
        p['st_uid'] = uid.data
        p['st_gid'] = gid.data

    def create(self, path, mode):
        hashv = self.hashVal(path.data)
        p, tar = self.traverseparent(path.data)
        p['files'][tar] = dict(st_mode=(S_IFREG | int(mode.data)), stfacnk=1,
                     st_size=0, st_ctime=time(), st_mtime=time(),
                     st_atime=time(), blocks=[], hash_val = hashv)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh = None):
        try:
            p = self.traverse(path.data)
        except KeyError:
            return pickle.dumps(-1)
        #print("returning attr")
        return pickle.dumps({attr:p[attr] for attr in p.keys() if (attr != 'files' and attr != 'blocks')})

    def getxattr(self, path, name, position=0):
        p = self.traverse(path.data)
        attrs = p.get('attrs', {})
        try:
            return pickle.dumps(attrs[name.data])
        except KeyError:
            return pickle.dumps('')       # Should return ENOATTR

    def listxattr(self, path):
        p = self.traverse(path.data)
        attrs = p.get('attrs', {})
        return pickle.dumps(attrs.keys())

    def mkdir(self, path, mode):
        p, tar = self.traverseparent(path.data)
        p['files'][tar] = dict(st_mode=(S_IFDIR | int(mode.data)), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),files={})
        p['st_nlink'] += 1
        print("mkdir success")

    def open(self, path, flags):
        self.fd += 1
        return pickle.dumps(self.fd)

    def read(self, bin_path, bin_size, bin_offset):
        #print("read")
        path = bin_path.data
        size = int(bin_size.data)
        offset = int(bin_offset.data)
        fp = self.traverse(path)
        start_blk = offset//MaxBLOCKSIZE
        last_block = (offset + size)//MaxBLOCKSIZE
        retBlocks = fp['blocks'][start_blk : last_block+1]
        #print(retBlocks)
        return pickle.dumps(retBlocks)

    def readdir(self, path, fh):
        p = self.traverse(path.data)['files']
        return pickle.dumps(['.', '..'] + [x for x in p ])

    def readlink(self, path):
        p = self.traverse(path.data)
        return Binary(p['blocks'][0])

    def removexattr(self, path, name):
        p = self.traverse(path.data)
        attrs = p.get('attrs', {})
        try:
          del attrs[name]
        except KeyError:
          return Binary('1')        # Should return ENOATTR
        return Binary('0')

    def rename(self, old, new):
        po, po1 = self.traverseparent(old.data)
        pn, pn1 = self.traverseparent(new.data)
        if po['files'][po1]['st_mode'] & 0o770000 == S_IFDIR:
            po['st_nlink'] -= 1
            pn['st_nlink'] += 1
        pn['files'][pn1] = po['files'].pop(po1)
        
    def rmdir(self, path):
        p, tar = self.traverseparent(path.data)
        if len(p['files'][tar]['files']) > 0:
            return Binary('1')
        p['files'].pop(tar)
        p['st_nlink'] -= 1
        return Binary('0')

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        p = self.traverse(path.data)
        attrs = p.setdefault('attrs', {})
        attrs[name.data] = value.data

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        #change: hash value required because new file is created
        hashv = self.hashVal(target.data)
        p, tar = self.traverseparent(target.data)
        p['files'][tar] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
        st_size=len(source.data), blocks = [source.data], hash_val = hashv)

    def truncate(self, bin_path, bin_length, fh = None):
        length = int(bin_length.data)
        path = bin_path.data
    	#print("*** length = ", length)
    	#print("file data = ",d)
        p = self.traverse(path)
        p['st_size'] = length
        blocks = p['blocks']
        num_blocks = length//MaxBLOCKSIZE
        keep_blocks_num = num_blocks if length%MaxBLOCKSIZE else num_blocks+1
        p['blocks'] = blocks[:keep_blocks_num]
        return pickle.dumps(blocks[num_blocks:])

    def unlink(self, path):
        #print(self.files)
        p, tar = self.traverseparent(path.data)
        #For symbolic links unlink is limited to metaserver, hence no need to return blocks
        if(p['files'][tar]['st_mode'] == (S_IFLNK | 0o777)):
            p['files'].pop(tar)
            return pickle.dumps("symlink")
        #print("Blocks = ",p['files'][tar]['blocks'])
        blocks = p['files'][tar]['blocks']
        p['files'].pop(tar)
        return pickle.dumps(blocks)

    def utimens(self, path, times = None):
        now = time()
        atime, mtime = times if times else (now, now)
        p = self.traverse(path.data)
        p['st_atime'] = atime
        p['st_mtime'] = mtime

    def write(self, bin_path, bin_data, bin_offset):
        path = bin_path.data
        offset = int(bin_offset.data)
        data = bin_data.data
        data_size = len(data)
        p = self.traverse(path) #file pointer
        #d, d1 = self.traverseparent(path) #d = parent pointer, d1=filename
        print(p)
        print(data)
        print(offset)
    	print("file_data",self.data)
    	file_size  = p['st_size']
        print('original file size:' + str(file_size))
        hash_val = p['hash_val']
        blockIds = []
        if(len(p['blocks']) == 0):
            #first time write
            print('file did not exist earlier, writing first time')
            data_size = data_size + offset
            num_blocks = data_size//MaxBLOCKSIZE if (data_size % MaxBLOCKSIZE) == 0 else data_size//MaxBLOCKSIZE +1
            for i in range(0,num_blocks):
                blockID = hash_val + '_' + str(i)
                blockIds.append(blockID)
            p['blocks'] = blockIds
            p['st_size'] = data_size
            print('num of blocks:' , len(blockIds))
            return pickle.dumps(blockIds)
        else:
            print('file already exists')
            blockIds = p['blocks']
            if(offset > file_size):
                #need to append the file with data
                print('need to write beyond actual file size, here comes the /x00s')
                last_block = len(blockIds)
                data_size = (offset - file_size%MaxBLOCKSIZE) + data_size
                num_new_blocks = data_size//MaxBLOCKSIZE if (data_size % MaxBLOCKSIZE) == 0 else data_size//MaxBLOCKSIZE +1
                for i in range(0, num_new_blocks):
                    blockIds.append(hash_val + '_' + str(last_block+i))
                p['blocks'] = blockIds
                p['st_size'] = data_size
                return pickle.dumps(blockIds)
            else:
                print('need to overwite some data in between')
                edge_block = 0 if offset==0 else offset//MaxBLOCKSIZE
                retain_blocks = blockIds[:edge_block]
                if(file_size < offset + data_size):
                    last_block = len(blockIds)
                    new_size = offset + data_size
                    extra_data = new_size - len(blockIds)*MaxBLOCKSIZE
                    num_new_blocks =  extra_data//MaxBLOCKSIZE if (extra_data % MaxBLOCKSIZE) == 0 else extra_data//MaxBLOCKSIZE +1
                    for i in range(0, num_new_blocks):
                        blockIds.append(hash_val + '_' + str(last_block+i))
                    p['blocks'] = blockIds
                    p['st_size'] = new_size
                    return pickle.dumps(blockIds)
                else:
                    return pickle.dumps(blockIds)


def main():
  
  serve(int(sys.argv[1]))

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port), allow_none = True)
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.chmod)
  file_server.register_function(sht.chown)
  file_server.register_function(sht.create)
  file_server.register_function(sht.create)
  file_server.register_function(sht.getattr)
  file_server.register_function(sht.getxattr)
  file_server.register_function(sht.listxattr)
  file_server.register_function(sht.mkdir)
  file_server.register_function(sht.open)
  file_server.register_function(sht.read)
  file_server.register_function(sht.readdir)
  file_server.register_function(sht.readlink)
  file_server.register_function(sht.removexattr)
  file_server.register_function(sht.rename)
  file_server.register_function(sht.rmdir)
  file_server.register_function(sht.setxattr)
  file_server.register_function(sht.statfs)
  file_server.register_function(sht.symlink)
  file_server.register_function(sht.truncate)
  file_server.register_function(sht.unlink)
  file_server.register_function(sht.utimens)
  file_server.register_function(sht.write)
  file_server.register_function(sht.gethashVal)

  print("Server Running")
  file_server.serve_forever()


if __name__ == "__main__":
  main()