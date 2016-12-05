import xmlrpclib
import sys
import hashlib
from xmlrpclib import Binary

ms_port = sys.argv[1]
server_url = "http://127.0.0.1:" + str(ms_port)
ms_helper = xmlrpclib.Server(server_url)
filepath = str(sys.argv[-1])
print(filepath)
blk_id = ms_helper.getDataBlocksIDs(Binary(filepath))
if(not(blk_id == False)):
	ds_ports = sys.argv[2:-1]
	for ds_port in ds_ports:
		server_url = "http://127.0.0.1:" + str(ds_port)
	  	#ms_helper = Helper(xmlrpclib.Server(server_url))
		ds_helper = xmlrpclib.Server(server_url)
		if(ds_helper.corrupt(str(blk_id))):
			print("Corrupted block {} on server with port {}".format(filepath, ds_port))
else:
	print("Block id not found")