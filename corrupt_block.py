import xmlrpclib
import sys
import hashlib

def hashVal(path):
        '''
        method to generate hash value on passing path
        '''
        return str(int(hashlib.md5(path).hexdigest()[:8],16))

ds_ports = sys.argv[1:-1]
for ds_port in ds_ports:
	server_url = "http://127.0.0.1:" + str(ds_port)
  	#ms_helper = Helper(xmlrpclib.Server(server_url))
	ds_helper = xmlrpclib.Server(server_url)
	block_id = hashVal(str(sys.argv[-1])) + "_0"
	print(block_id)
	ds_helper.corrupt(str(block_id))