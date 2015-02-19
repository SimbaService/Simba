#!/usr/bin/env python

import os
import sys
import probe_config as conf
import time
simba_disk = 'sda4'
base_dir = '/mnt/sda4'

def prep_simba_partition():
	for n in conf.proxy_nodes:
		print 'Creating partition on '  + n
		os.system("ssh -o StrictHostKeyChecking=no %s sudo %s/mkfs_and_mount.sh %s %s" % (n, conf.script_dir, simba_disk, base_dir))
		os.system("ssh -o StrictHostKeyChecking=no %s sudo chgrp %s %s" % (n, conf.proj, base_dir))
		os.system("ssh -o StrictHostKeyChecking=no %s sudo chmod g+w %s" % (n, base_dir))
	for n in conf.gateway_nodes:
		print 'Creating partition on '  + n
		os.system("ssh -o StrictHostKeyChecking=no %s sudo %s/mkfs_and_mount.sh %s %s" % (n, conf.script_dir, simba_disk, base_dir))
		os.system("ssh -o StrictHostKeyChecking=no %s sudo chgrp %s %s" % (n, conf.proj, base_dir))
		os.system("ssh -o StrictHostKeyChecking=no %s sudo chmod g+w %s" % (n, base_dir))
	for n in conf.other_nodes:
		print 'Creating partition on '  + n
		os.system("ssh -o StrictHostKeyChecking=no %s sudo %s/mkfs_and_mount.sh %s %s" % (n, conf.script_dir, simba_disk, base_dir))
		os.system("ssh -o StrictHostKeyChecking=no %s sudo chgrp %s %s" % (n, conf.proj, base_dir))
		os.system("ssh -o StrictHostKeyChecking=no %s sudo chmod g+w %s" % (n, base_dir))

def prep_simbastore_properties():
	s="""
# General
port=%d

thread.count=128

#gateway management (TTL = ms)
gateway.ttl=6000000

# Object Store (Swift)
swift.identity=simba:simba
swift.password=simba123
swift.proxy.url=http://localhost:8080/auth/v1.0
swift.container=simbastore

# Table Store (Cassandra)
cassandra.seed=%s
cassandra.keyspace=simbastore

# Operation Log
oplog.name=operation.log
oplog.enable=true

# Operation Cache
cache.size.limit=1000
cache.enable=true

# Chunk Cache
chunkcache.size_mb=500
chunkcache.enable=true
""" % (conf.simbastore_port, conf.cassandra_nodes[0])
	with open('simbastore.properties', 'w') as outfile:
		outfile.write(s)

def prep_gateway_properties():
	s="""
port=%d
buffer.size=1048576
bitmap.notification=true
http.port=9090
""" % conf.gateway_port
	with open('gateway.properties', 'w') as outfile:
		outfile.write(s)
		outfile.write("simbastores=%s:%d"% (conf.proxy_nodes[0], conf.simbastore_port))
		print str(conf.proxy_nodes)
		for n in conf.proxy_nodes[1:]:
			outfile.write(",%s:%d" % (n, conf.simbastore_port))
		outfile.write("\n")
		outfile.write("backend.server.thread.count=%d\n" % len(conf.swift_nodes))
		outfile.write("frontend.compression=true")

def prep_client_properties():
	with open('client.properties', 'w') as outfile:
		outfile.write("gateways=")
		for n in conf.gateway_nodes:
			outfile.write("%s:%d," % (n, conf.gateway_port))
		outfile.write("\n")


def start_simbastores():
	for n in conf.proxy_nodes:
		print "Starting simbastore on " + n
		os.system("ssh -o StrictHostKeyChecking=no %s %s/simbastore.sh start" % (n, conf.script_dir))

def stop_simbastores():
	for n in conf.proxy_nodes:
		print "Stopping simbastore on " + n
		os.system("ssh -o StrictHostKeyChecking=no %s %s/simbastore.sh stop" % (n, conf.script_dir))

def start_gateways():
	for n in conf.gateway_nodes:
		print "Starting gateway on " + n
		os.system("ssh -o StrictHostKeyChecking=no %s %s/gateway.sh start" % (n, conf.script_dir))

def stop_gateways():
	for n in conf.gateway_nodes:
		print "Stopping gateway on " + n
		os.system("ssh -o StrictHostKeyChecking=no %s %s/gateway.sh stop" % (n, conf.script_dir))


if __name__ == "__main__":

	if (len(sys.argv) != 3):
		print "usage %s [start|stop|prep] [simbastore|gateway|all]\n"
		print "prep only prepares the properties files"
		sys.exit(0)
	elif sys.argv[1] == "start":
		prep_simbastore_properties()
		prep_gateway_properties()
		prep_client_properties()
		if sys.argv[2] == "all" or sys.argv[2] == "simbastore":
			start_simbastores()
			time.sleep(10)
		if sys.argv[2] == "all" or sys.argv[2] == "gateway":
			start_gateways()
	elif sys.argv[1] == "stop":
		if sys.argv[2] == "all" or sys.argv[2] == "gateway":
			stop_gateways()
		if sys.argv[2] == "all" or sys.argv[2] == "simbastore":
			stop_simbastores()
	elif sys.argv[1] == "prep":
		prep_simba_partition()
		prep_simbastore_properties()
		prep_gateway_properties()
		prep_client_properties()
	
