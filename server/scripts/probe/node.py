#!/usr/bin/python

import socket
import probe_config as conf
import cassandra
import swift
import other
import os
import sys

class Node:
	def __init__(self):
		self.myname = socket.gethostname()
		self.ip = socket.gethostbyname(self.myname)
		print self.ip

	def _getNode(self, op):
		n = None
		if self.myname in conf.cassandra_nodes:
			n = cassandra.Cassandra(self.myname)
			self._doOperation(n, op)
		if self.myname in conf.swift_nodes:
			n = swift.Swift(self.myname, True)
			self._doOperation(n, op)
		if self.myname in conf.proxy_nodes:
			n = swift.Swift(self.myname, False)
			self._doOperation(n, op)
		if self.myname in conf.other_nodes:
			n = other.Other(self.myname)
			self._doOperation(n, op)
		
	def _doOperation(self, n, op):
		if n != None:
			if op == "configure":
				n.configure()
			if op == "start":
				n.start()
			if op == "stop":
				n.stop()

	def configure(self):
		n = self._getNode("configure")

	def start(self):
		n = self._getNode("start")

	def stop(self):
		n = self._getNode("stop")
			
if __name__ == '__main__':
	n = Node()
	if len(sys.argv) > 1:
		if sys.argv[1] == "configure":
			print 'CONFIGURE NODE'
			n.configure()
		elif sys.argv[1] == "start":
			print 'START NODE'
			n.start()
		elif sys.argv[1] == "stop":
			n.stop()
	else:
		print "usage: node.py [configure|start|stop]"
