#!/usr/bin/env python

import probe_config as conf
import os

# This will create the ring files on the first swift node
print "Configuring node %s" % conf.swift_nodes[0]
os.system('ssh -o StrictHostKeyChecking=no %s sudo %s/configure.sh' % (conf.swift_nodes[0], conf.script_dir))

for n in conf.swift_nodes[1:] + conf.proxy_nodes + conf.cassandra_nodes + conf.other_nodes:
	print "Configuring node %s" % n
	os.system("ssh -o StrictHostKeyChecking=no %s sudo %s/configure.sh" % (n, conf.script_dir))

for n in conf.swift_nodes + conf.proxy_nodes + conf.cassandra_nodes + conf.other_nodes:
	print "Starting node %s" % n
	os.system("ssh -o StrictHostKeyChecking=no %s sudo %s/start.sh" % (n, conf.script_dir))

print "Cluster configuration"
print 'Swift Storage Nodes:'
for n in conf.swift_nodes:
	print "\t%s" % n
print 'Proxy/SimbaStore nodes:'
for n in conf.proxy_nodes:
	print "\t%s" % n
print 'Cassandra nodes:'
for n in conf.cassandra_nodes:
	print "\t%s" % n
print 'Other nodes:'
for n in conf.other_nodes:
	print "\t%s" % n
print 'Gateway nodes:'
for n in conf.gateway_nodes:
	print "\t%s" % n
