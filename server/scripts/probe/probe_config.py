#!/usr/bin/python

#################################################
# YOU MUST SET THE PROJECT AND EXPERIMENT NAMES #
#################################################

proj='proj'
exp='exp'

#################################################

script_dir = '/proj/'+proj+'/scripts/'
base_name = '.'+proj+'.'+exp+'.kodiak.nx'
data_disk = 'sdb'
simbastore_port = 9000
gateway_port = 9001

def to_hostnames(r):
	return ["h%d%s" % (x, base_name) for x in r]

all_nodes = range(0,48)
excluded_nodes = set([])
all_nodes = filter(lambda p: p not in excluded_nodes,all_nodes)

swift_nodes = to_hostnames(all_nodes[0:16]) #swift nodes
proxy_nodes = to_hostnames(all_nodes[0:16]) # simba stores
cassandra_nodes = to_hostnames(all_nodes[16:32]) 
gateway_nodes = to_hostnames(all_nodes[16:32]) 
other_nodes = to_hostnames(all_nodes[32:48]) # client hosts

# this is the partition power, # of partitions = 2^(swift_num_partitions)
# should be approximately 100 * # disks
swift_num_partitions = 11

if __name__ == "__main__":
	print "swift nodes=" + str(swift_nodes)
	print "\n"
	print "cassandra nodes=" + str(cassandra_nodes)
	print "\n"
	print "proxy nodes=" + str(proxy_nodes)
	print "\n"
	print "other nodes=" + str(other_nodes)
	print "\n"
	print "gateway nodes=" + str(gateway_nodes)
	print "\n"
