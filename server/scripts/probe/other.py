#!/usr/bin/python

import re
import probe_config as conf
import socket
import os

class Other:

	def __init__(self, myname):
		self.myname = myname
		self.base_dir = '/mnt/%s1' % conf.data_disk

        def _configure_limits(self):
                s = """
* soft nofile 262144
* hard nofile 262144
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
fs.file-max = 262144
"""
                with open('/etc/sysctl.conf','w') as outfile:
                        outfile.write(s)
                os.system('sysctl -p')

	def configure(self):
		# DISABLED. NOT USING conf.data_disk FOR CLIENT HOSTS.
        # CHECK simba.py TO CHANGE DISK USED FOR CLIENT HOSTS.
        #os.system("./partition.sh %s %s" % (conf.data_disk, self.base_dir))
		#os.system("chgrp %s %s" % (conf.proj, self.base_dir))
		#os.system("chmod g+w %s" % self.base_dir)
		self._configure_sysctl()
		self._configure_limits()

	def start(self):
		return
