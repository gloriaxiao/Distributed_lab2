#!/usr/bin/env python
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
from threading import Lock, Thread

LEADER_BASEPORT = 25000
SLEEP = 0.05
ADDR = 'localhost'


class LeaderListenerToReplica(Thread): 
	def __init__(self, pid, target_pid, num_servers): 
		Thread.__init__(self)
		pass 

	def run(self): 
		pass 


class LeaderSenderToReplica(Thread): 
	def __init__(self, pid, target_pid, num_servers): 
		Thread.__init__(self)
		pass 

	def run(self): 
		pass 

	def send(type, msg): 
		pass 


class LeaderListenerToAcceptor(Thread): 
	def __init__(self, pid, target_pid, num_servers): 
		Thread.__init__(self)
		pass 

	def run(self): 
		pass 


class LeaderSenderToAcceptor(Thread): 
	def __init__(self, pid, target_pid, num_servers): 
		Thread.__init__(self)
		pass 

	def run(self): 
		pass 

	def send(type, msg): 
		pass 