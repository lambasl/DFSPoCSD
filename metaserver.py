import logging

from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib
from datetime import datetime, timedelta
from xmlrpclib import Binary
from time import time
from stat import S_IFDIR, S_IFLNK, S_IFREG
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

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
        p['st_mode'] |= mode.data
        return Binary(0)

    def chown(self, path, uid, gid):
        p = self.traverse(path.data)
        p['st_uid'] = uid.data
        p['st_gid'] = gid.data

    def create(self, path, mode):
        p, tar = self.traverseparent(path.data)
        p['files'][tar] = dict(st_mode=(S_IFREG | int(mode.data)), st_nlink=1,
                     st_size=0, st_ctime=time(), st_mtime=time(),
                     st_atime=time())
        self.fd += 1
        return self.fd

    def getattr(self, path, fh = None):
        try:
            p = self.traverse(path.data)
        except KeyError:
            return pickle.dumps(-1)
        print("returning attr")
        return pickle.dumps({attr:p[attr] for attr in p.keys() if attr != 'files'})

    def getxattr(self, path, name, position=0):
        p = self.traverse(path.data)
        attrs = p.get('attrs', {})
        try:
            return pickle.dumps(attrs[name.data])
        except KeyError:
            return pickle.dumps('')       # Should return ENOATTR

    def listxattr(self, path):
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        p, tar = self.traverseparent(path.data)
        p['files'][tar] = dict(st_mode=(S_IFDIR | int(mode.data)), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),files={})
        p['st_nlink'] += 1
        print("mkdir success")
        #Below lines are not needed, handle creation in write function itself
        # d, d1 = self.traverseparent(path, True)
        # d[d1] = defaultdict(bytes)

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        d = self.traverse(path, True)
    	#case: offset > filesize
    	if((offset//MaxBLOCKSIZE + 1) > len(d)):
    	    return ''

    	#get first block where offset lies
    	b_no = offset // MaxBLOCKSIZE
    	data = ""
    	data = d[b_no]
    	# Case 1 : data is within one block
    	if(size < MaxBLOCKSIZE):
    	    return data[offset%MaxBLOCKSIZE : offset%MaxBLOCKSIZE + size]

    	# Case 2 : data spans over more than 1 block
    	else:
    	    data = data[offset%MaxBLOCKSIZE :]
    	    no_of_blocks = len(d)
    	    if(b_no+size//MaxBLOCKSIZE < len(d)):
    		no_of_blocks = b_no+size//MaxBLOCKSIZE
    	    for i in range(b_no+1, no_of_blocks):
    		data += d[i]
    	    if(size - len(data) > 0 and no_of_blocks < len(d)):
    		data+= d[(offset+size)//MaxBLOCKSIZE][:size - len(data)]
    	    return data
    	##
    	#data = ''.join(d)
            #return data[offset:offset + size]

    def readdir(self, path, fh):
        p = self.traverse(path.data)['files']
        return pickle.dumps(['.', '..'] + [x for x in p ])

    def readlink(self, path):
        return self.traverse(path, True)

    def removexattr(self, path, name):
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        po, po1 = self.traverseparent(old)
        pn, pn1 = self.traverseparent(new)
        if po['files'][po1]['st_mode'] & 0o770000 == S_IFDIR:
            po['st_nlink'] -= 1
            pn['st_nlink'] += 1
        pn['files'][pn1] = po['files'].pop(po1)
        do, do1 = self.traverseparent(old, True)
        dn, dn1 = self.traverseparent(new, True)
        dn[dn1] = do.pop(do1)

    def rmdir(self, path):
        p, tar = self.traverseparent(path)
        if len(p['files'][tar]['files']) > 0:
            raise FuseOSError(ENOTEMPTY)
        p['files'].pop(tar)
        p['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        p = self.traverse(path)
        attrs = p.setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        p, tar = self.traverseparent(target)
        p['files'][tar] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))
        d, d1 = self.traverseparent(target, True)
        d[d1] = source

    def truncate(self, path, length, fh = None):
    	print("*** length = ", length)
    	#print("file data = ",d)
        d,d1 = self.traverseparent(path, True)
    	no_of_blocks = (length // MaxBLOCKSIZE) + 1
    	d[d1] = d[d1][:no_of_blocks]
    	d[d1][-1] = d[d1][-1][:length % MaxBLOCKSIZE]
        p = self.traverse(path)
        p['st_size'] = length

    def unlink(self, path):
        p, tar = self.traverseparent(path)
        p['files'].pop(tar)
    	d, d1 = self.traverseparent(path, True)
    	d[d1] = ['']

    def utimens(self, path, times = None):
        now = time()
        atime, mtime = times if times else (now, now)
        p = self.traverse(path)
        p['st_atime'] = atime
        p['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        p = self.traverse(path)
        d, d1 = self.traverseparent(path, True)
    	if not d[d1]:
	    #print("Write: empty list created")
	       d[d1] = ['']
    	#print("file_data",self.data)
    	file_size  = p['st_size']
    	
    	#for testing	
    	#offset = 5
    	#d[d1].append("helloworld")
    	#file_size = 11
    	##
    	
    	#print("previous file size=",file_size)

    	# Case 1: append null chars if offset > filesize
    	if(offset > file_size):
    	    null_chars = "\x00"*(offset - file_size)
    	    extra_data = self.format_data(null_chars, file_size)
    	    last_block_no = (file_size)//MaxBLOCKSIZE
    	    if((file_size+1) % MaxBLOCKSIZE > 0):
    	        d[d1][last_block_no] += extra_data[0]
    	        extra_data.pop(0)
      	    for i in extra_data:
    	        d[d1].append(i)
    	    Block_no = offset//MaxBLOCKSIZE
    	    formatted_data = self.format_data(data, offset)
    	    if(offset % MaxBLOCKSIZE > 0):
    	        d[d1][Block_no] += formatted_data[0]
    	        formatted_data.pop(0)
    	    for i in formatted_data:
    	        d[d1].append(i)
    	##
    	#Case 2: len(data)+offset <= file_size 
    	elif(len(data)+offset <= file_size):
    	    present_data = ''.join(d[d1])
    	    p1 = present_data[:offset]
    	    p2 = present_data[offset+len(data):]
    	    new_data = p1 + data + p2
    	    new_data_listview = [new_data[i:i+MaxBLOCKSIZE] for i in range(0, len(new_data), MaxBLOCKSIZE)]
    	    d[d1] = new_data_listview
    	
    	#Case 3: offset < filesize and len(data)+offset > file_size 
    	else:
    	    Block_no = offset//MaxBLOCKSIZE
    	    formatted_data = self.format_data(data, offset)
    	    if(offset % MaxBLOCKSIZE > 0):
    	        d[d1][Block_no] += formatted_data[0]
    	        formatted_data.pop(0)
    	    for i in formatted_data:
    		if(offset == 0):
    		    d[d1].pop(0)
    	        d[d1].append(i)
    		
    	current_data = ''.join(d[d1])
        p['st_size'] = len(current_data)
    	#print("file_data_after write",self.data)
        return len(data)

    def format_data(self, data, offset):
        Block_no = offset//MaxBLOCKSIZE
        Offset_in_block = offset % MaxBLOCKSIZE	
        formatted_data = []
        if(Offset_in_block > 0):
            formatted_data.append(data[0:MaxBLOCKSIZE - Offset_in_block])
            data = data[MaxBLOCKSIZE - Offset_in_block : ]
        data_len = len(data)
        data = [ data[i:i+MaxBLOCKSIZE] for i in range(0, data_len, MaxBLOCKSIZE) ]
        formatted_data += data
        return formatted_data

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

  print("Server Running")
  file_server.serve_forever()


if __name__ == "__main__":
  main()