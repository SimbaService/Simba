#!/usr/bin/python

import re
import probe_config as conf
import socket
import os
import tempfile
import time

class Cassandra:

	def __init__(self, myname):
		self.myname = myname
		self.allnodes = conf.cassandra_nodes
		self.idx = self.allnodes.index(myname)
		self.base_dir = '/mnt/%s1' % conf.data_disk

        def _configure_limits(self):
                s = """
* soft nofile 999999
* hard nofile 999999
"""
                with open('/etc/security/limits.conf','a') as outfile:
                        outfile.write(s)

        def _configure_sysctl(self):
                s = """
# disable TIME_WAIT.. wait..
net.ipv4.tcp_tw_recycle=1
net.ipv4.tcp_tw_reuse=1

# disable syn cookies
net.ipv4.tcp_syncookies = 0

# double amount of allowed conntrack
net.ipv4.netfilter.ip_conntrack_max = 262144
net.core.rmem_max = 8388608
net.core.wmem_max = 8388608
net.core.rmem_default = 65536
net.core.wmem_default = 65536
net.ipv4.tcp_rmem = 4096 87380 8388608
net.ipv4.tcp_wmem = 4096 65536 8388608
net.ipv4.tcp_mem = 8388608 8388608 8388608
net.ipv4.ip_local_port_range = 15000 61000
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_recycle = 1 
net.ipv4.tcp_tw_reuse = 1 
net.core.somaxconn = 32768
net.ipv4.tcp_max_syn_backlog = 10240
net.core.netdev_max_backlog = 10240
fs.file-max = 999999
"""
                with open('/etc/sysctl.conf','w') as outfile:
                        outfile.write(s)
                os.system('sysctl -p')

	def configure(self):
		os.system("./partition.sh %s %s" % (conf.data_disk, self.base_dir))
		self._configure_sysctl()
		self._configure_limits()
		num_cass_nodes = len(self.allnodes)
		token = str(((2**64 / num_cass_nodes) * self.idx) - 2**63)
		seed_ip = socket.gethostbyname(self.allnodes[0])
		my_ip = socket.gethostbyname(self.myname)
		# read in template
		with open("cassandra.yaml", "r") as infile:
			inlines = infile.readlines()
		with open("/opt/apache-cassandra-1.2.5/conf/cassandra.yaml", "w") as outfile:
			for line in inlines:
				line = re.sub(r'__BASE_DIR__', self.base_dir, line)
				line = re.sub(r'__INITIAL_TOKEN__', token, line)
				line = re.sub(r'__MY_IP__', my_ip, line)
				line = re.sub(r'__SEED_IP__', seed_ip, line)
				outfile.write(line)
		# UNCOMMENT IF YOU MAKE CHANGES TO THE DEFAULT 'cassandra-env.sh'
        #with open("cassandra-env.sh", "r") as infile:
		#	inlines = infile.readlines()
		#with open("/opt/apache-cassandra-1.2.5/conf/cassandra-env.sh", "w") as outfile:
		#	for line in inlines:
		#		outfile.write(line)

	def _initialize_db(self):
		print "initializing db"
		time.sleep(20)
		print "finished sleeping..."
		init="""\
create KEYSPACE simbastore WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

create table simbastore.subscriptions (key uuid PRIMARY KEY, subscriptions list<blob>);

create table simbastore.metadata (key text PRIMARY KEY, consistency text);

create KEYSPACE test_3replicas WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
"""
		fh, path = tempfile.mkstemp()
		with open(path,'w') as outfile:
			outfile.write(init)
		os.close(fh)
		os.system("/opt/apache-cassandra-1.2.5/bin/cqlsh -f %s" % path)
		os.unlink(path)

	def start(self):
		os.system('/opt/apache-cassandra-1.2.5/bin/cassandra')
		if self.myname == self.allnodes[-1]:
			self._initialize_db()

	def stop(self):
		os.system('pkill -f cassandra')

