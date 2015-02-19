#!/usr/bin/python

import probe_config as conf
import socket
import re
import os
import tempfile
import shutil

class Swift:
	def __init__(self, myname, is_storage):
		self.myname = myname
		print "Myname = " + self.myname
		self.allnodes = conf.swift_nodes
		print "all nodes=" + str(self.allnodes)
		self.all_ips = [socket.gethostbyname(x) for x in self.allnodes]
		self.my_ip = socket.gethostbyname(self.myname)
		self.base_dir = '/srv/node/%s1' % conf.data_disk
		self.is_storage = is_storage


	def _grep(self, needle, filename):
		with open(filename, "r") as infile:
			for line in infile:
				if re.search(needle, line):
					return True
		return False

	def _append_to_file(self, line, filename):
		with open(filename, "a") as outfile:
			outfile.write(line)

	def _initialize_container(self):
		print "Initializing container"
		os.system('swift -A http://localhost:8080/auth/v1.0 -U simba:simba -K simba123 post simbastore')

	def _replace_in_file(self, before, after, filename):
		with open(filename, "r") as infile:
			lines = infile.readlines()
		fh, path = tempfile.mkstemp()
		with open(path, 'w') as outfile:
			for line in lines:
				line = re.sub(before, after, line)
				outfile.write(line)
		os.close(fh)
		os.rename(path, filename)

	def _build_ring(self, ring_type, port):
		b = "%s.builder" % ring_type
		dev = "%s1" % conf.data_disk
		os.system("swift-ring-builder %s create %d 3 1" % (b, conf.swift_num_partitions))
		znum=1
		for node in self.all_ips:
			os.system("swift-ring-builder %s add z%d-%s:%d/%s 100" % (b, znum, node, port, dev))
			znum += 1
		os.system("swift-ring-builder %s" % b)
		os.system("swift-ring-builder %s rebalance" % b)

	def _build_rings(self):
		print 'self.all_ips[0]==', self.all_ips[0]
		print 'self.my_ip==', self.my_ip
		if self.my_ip == self.all_ips[0]:
			self._build_ring('account', 6002)
			self._build_ring('container', 6001)
			self._build_ring('object', 6000)
		shutil.copy2('account.ring.gz', '/etc/swift')
		shutil.copy2('container.ring.gz', '/etc/swift')
		shutil.copy2('object.ring.gz', '/etc/swift')
		os.system('chown -R swift:swift /etc/swift')
		
		
	def _configure_limits(self):
		s = """
* soft nofile 999999
* hard nofile 999999
"""
                with open('/etc/security/limits.conf','a') as outfile:
                        outfile.write(s)
                os.system('sysctl -p')
	
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

			

	def _update_users(self):
		if not self._grep('swift', '/etc/passwd'):
			self._append_to_file('swift:x:109:120::/home/swift:/bin/false', '/etc/passwd')
		if not self._grep('swift', '/etc/group'):
			self._append_to_file('swift:x:120:', '/etc/group')
			os.system('mkdir -p /home/swift')
			os.system('chown swift:swift /home/swift')

	def _configure_rsync(self):
		s="""
uid = swift
gid = swift
log file = /var/log/rsyncd.log
pid file = /var/run/rsyncd.pid
address = %s

[account]
max connections = 2
path = /srv/node/
read only = false
lock file = /var/lock/account.lock

[container]
max connections = 2
path = /srv/node/
read only = false
lock file = /var/lock/container.lock

[object]
max connections = 2
path = /srv/node/
read only = false
lock file = /var/lock/object.lock
		""" % self.my_ip
		with open('/etc/rsyncd.conf', 'w') as outfile:
			outfile.write(s)
		self._replace_in_file('RSYNC_ENABLE=false', 'RSYNC_ENABLE=true', '/etc/default/rsync')

	def _configure_account_server(self):
		if not os.path.exists('/etc/swift'):
			os.makedirs('/etc/swift') 
		s= """\
[DEFAULT]
bind_ip = %s
workers = 2
devices=/srv/node

[pipeline:main]
pipeline = account-server

[app:account-server]
use = egg:swift#account

[account-replicator]
concurrency = 4

[account-auditor]

[account-reaper]
concurrency = 4\
""" % self.my_ip
		with open('/etc/swift/account-server.conf', 'w') as outfile:
			outfile.write(s)
	

	def _configure_container_server(self):
		if not os.path.exists('/etc/swift'):
			os.makedirs('/etc/swift') 
		s="""\
[DEFAULT]
bind_ip = %s
workers = 2
devices=/srv/node

[pipeline:main]
pipeline = container-server

[app:container-server]
use = egg:swift#container

[container-replicator]
concurrency = 4

[container-updater]
concurrency = 2

[container-auditor]

[container-sync]\
""" % self.my_ip
		with open('/etc/swift/container-server.conf', 'w') as outfile:
			outfile.write(s)

	def _configure_object_server(self):
		if not os.path.exists('/etc/swift'):
			os.makedirs('/etc/swift') 
		s="""\
[DEFAULT]
bind_ip = %s
workers = 4
devices=/srv/node

[pipeline:main]
pipeline = object-server

[app:object-server]
use = egg:swift#object
network_chunk_size=65536
disk_chunk_size=65536
threads_per_disk=4
replication_concurrency=1

[object-replicator]
concurrency = 1

[object-updater]
concurrency = 1

[object-auditor]
files_per_second = 1
bytes_per_second = 65536
""" % self.my_ip
		with open('/etc/swift/object-server.conf', 'w') as outfile:
			outfile.write(s)

	def _configure_hash(self):
		s="""\
[swift-hash]
# random unique strings that can never change (DO NOT LOSE)
swift_hash_path_prefix = 256b3282f8acc0ee0dad2565d1ab670a 
swift_hash_path_suffix = 13409460ac1879aff0b161c750fa7db1
"""
		with open('/etc/swift/swift.conf', 'w') as outfile:
			outfile.write(s)

	def _configure_proxy_server(self):
		s="""\
[DEFAULT]
bind_port = 8080
workers = 8
user = swift

[pipeline:main]
pipeline = healthcheck cache tempauth proxy-server

[app:proxy-server]
use = egg:swift#proxy
allow_account_management = true
account_autocreate = true

[filter:tempauth]
use = egg:swift#tempauth
user_system_root = testpass .admin https://%s:8080/v1/AUTH_system
user_simba_simba = simba123 .admin http://%s:8080/v1/AUTH_system
token_life = 604800

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:cache]
use = egg:swift#memcache
""" % (self.my_ip, self.my_ip)
		all_proxy_nodes = [socket.gethostbyname(x) for x in conf.proxy_nodes]
		m = "memcache_servers = %s:11211," % all_proxy_nodes[0]
		for p in all_proxy_nodes[1:]:
			m += "%s:11211," % p
		m += '\n'
		with open('/etc/swift/proxy-server.conf', 'w') as outfile:
			outfile.write(s)
			outfile.write(m)
			
	


	def _configure_as_storage_node(self):
		self._update_users()
		os.system("./partition.sh %s %s" % (conf.data_disk, self.base_dir))
		os.system("chown swift:swift %s" % self.base_dir)
		self._configure_rsync()
		self._configure_account_server()
		self._configure_container_server()
		self._configure_object_server()
		self._configure_hash()
		self._build_rings()
		self._configure_sysctl()
		self._configure_limits()

	def _configure_as_proxy_node(self):
		self._update_users()
		# IF PROXY NODES = SWIFT NODES, LEAVE THIS COMMENTED OUT
		#os.system("./partition.sh %s %s" % (conf.data_disk, self.base_dir))
		#os.system("chgrp %s %s" % (conf.proj, self.base_dir))
		#os.system("chmod g+w %s" % self.base_dir)
		self._configure_proxy_server()
		self._replace_in_file('^-l.*', '-l %s' % self.my_ip, '/etc/memcached.conf')
		self._configure_hash()
		self._build_rings()
		self._configure_sysctl()


	def _start_proxy_node(self):
		os.system("service memcached stop")
		os.system("service memcached start")
		os.system('swift-init proxy start')
		if self.myname == self.allnodes[-1]:
			self._initialize_container()

	def _start_storage_node(self):
		os.system("service rsync restart")
		os.system('swift-init all start')

	def configure(self):
		print 'Configure swift...'
		if self.is_storage:
			print 'Configure as Storage Node'
			self._configure_as_storage_node()
		else:
			print 'Configure as Proxy Node'
			self._configure_as_proxy_node()

	def start(self):
		if self.is_storage:
			print 'Start Storage Node'
			self._start_storage_node()
		else:
			print 'Start Proxy Node'
			self._start_proxy_node()
	def stop(self):
		os.system('swift-init all stop')
		if not self.is_storage:
			os.system('service memcached stop')	
