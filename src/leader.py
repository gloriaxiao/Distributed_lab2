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

p1b_msgs = []
p1b_lock = Lock() 

ballet_num = 0 
active = False 
proposals = set() 

def append_to_list(lst, lock, msg): 
	lock.acquire() 
	lst.append(msg)
	lock.release()

def pop_head_list(lst, lock): 
	lock.acquire()
	head = lst[0]
	lst = lst[1:]
	lock.release()
	return head 

def len_list(lst, lock): 
	lock.acquire()
	length = len(lst)
	lock.release()
	return length

def send_adopted_to_leader(b, pvalues): 
	pass 

def send_preempted_to_leader(b): 
	pass 

def spawnScout(b): 
	waitfor = set(listener_to_acceptors.keys()) 
	pvalues = set() 
	for i in sender_to_acceptors: 
		sender_to_acceptors[i].send("p1a " + str(ballot_number))
	while True: 
		if len_list(p1b_msgs, p1b_lock) != 0: 
			alpha, msg = pop_head_list(p1b_msgs, p1b_lock)
			b_prime, r = msg.split(" ", 1)
			if b == b_prime: 
				pvalues = pvalues.union(r)
				set.discard(alpha)
				if len(waitfor) < max_num_servers / 2: 
					send_adopted_to_leader(b, pvalues)
					return 
			else: 
				send_preempted_to_leader(b_prime)
				return

def spawnCommander(b, s, p): 
	waitfor = set(listener_to_acceptors.keys()) 
	for i in sender_to_acceptors: 
		sender_to_acceptors[i].send("p2a " + str(ballot_number) + " " + str(s) + " " + str(p))
	while True: 
		if len_list(p2b_msgs, p2b_lock) != 0: 
			alpha, b = pop_head_list(p2b_msgs, p2b_lock)
			if b_prime != b: 
				waitfor.discard(alpha)
				if len(waitfor) < len(max_num_servers) < 2: 
					for i in sender_to_replicas: 
						sender_to_replicas[i].send("decision " + str(s) + " " + str(p))
						return 
			else: 
				send_preempted_to_leader(b_prime)
				return


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
	spawnScout(ballot_number)

class Commander:
	def __init__(self):
		pass

class Scout:
	def __init__(self):
		pass


class LeaderListenerToReplica(Thread): 
	def __init__(self, pid, target_pid, num_servers): 
		Thread.__init__(self)
		self.pid = pid 
		self.target_pid = target_pid 
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = LEADER_BASEPORT + pid * num_servers + target_pid 
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self): 
		self.conn, self.addr = self.sock.accept() 
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest 
				cmd, arguments = l.split(" ", 1)
				if cmd == "propose": 
					s, p = arguments.split(" ", 1)
					found = False 
					for (s_prime, p_prime) in proposals: 
						if s == s_prime: 
							found = True 
							break 
					if not found: 
						proposals = proposals.union(set((s, p)))
						if active: 
							spawnCommander(ballot_number, s, p)
				else: 
					print "invalid command in ReplicaListenerToLeader"
			else: 
				try: 
					data = self.conn.recv(1024)
					if data == "": 
						raise ValueError
					self.buffer += data 
				except Exception as e:
					print str(self_pid) + " to " + str(self.target_pid) + " connection closed"
					self.conn.close()
					self.conn = None 
					self.conn, self.addr = self.sock.accept()


class LeaderSenderToReplica(Thread): 
	def __init__(self, pid, target_pid, num_servers): 
		Thread.__init__(self)
		self.pid = pid 
		self.target_pid = target_pid
		self.target_port = BASEPORT + target_pid * num_servers + pid 
		self.port = BASEPORT + pid * num_servers + num_servers + target_pid
		self.sock = None 

	def run(self): 
		pass 

	def send(self, msg): 
		try: 
			self.sock.send(msg)
		except Exception as e: 
			if self.sock: 
				self.sock.close() 
				self.sock = None 
			try: 
				new_socket = socket(AF_INET, SOCK_STREAM)
				new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				new_socket.connect((ADDR, self.target_port))
				self.sock = new_socket
			except: 
				time.sleep(SLEEP)


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