#!/usr/bin/env python

import probe_config as conf
import os

for n in conf.swift_nodes + conf.proxy_nodes + conf.cassandra_nodes + conf.other_nodes:
        print "Stopping node %s" % n
        os.system("ssh -o StrictHostKeyChecking=no %s sudo %s/stop.sh" % (n, conf.script_dir))

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
