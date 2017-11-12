#!/usr/bin/env python
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
from threading import Lock, Thread

LEADER_BASEPORT = 25000
SLEEP = 0.05
ADDR = 'localhost'
listener_to_replicas = {}
sender_to_replicas = {}
listener_to_acceptors = {}
sender_to_acceptors = {}


def init_leader(lid, num_servers):
	global listener_to_replicas, sender_to_replicas, listener_to_acceptors, sender_to_acceptors 
	for i in range(num_servers):
		listener_to_replicas[i] = LeaderListenerToReplica(lid, i, num_servers)
		listener_to_replicas[i].start()
	for i in range(num_leaders):
		sender_to_replicas[i] = LeaderSenderToReplica(lid, i, num_servers)
		sender_to_replicas[i].start()
	for i in range(num_servers):
		listener_to_acceptors[i] = LeaderListenerToAcceptor(lid, i, num_servers)
		listener_to_acceptors[i].start()
	for i in range(num_leaders):
		sender_to_acceptors[i] = LeaderSenderToAcceptor(lid, i, num_servers)
		sender_to_acceptors[i].start()


class Commander:
	def __init__(self):
		pass

class Scout:
	def __init__(self):
		pass


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