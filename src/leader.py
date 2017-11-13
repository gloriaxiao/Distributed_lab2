#!/usr/bin/env python
from pvalue import Pvalue
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
from threading import Lock, Thread, Condition

LEADER_BASEPORT = 25000
ACCEPTER_BASEPORT = 22500
BASEPORT = 20000
SLEEP = 0.05
ADDR = 'localhost'
NUM_SERVERS = 0
listener_to_replicas = {}
sender_to_replicas = {}
listener_to_acceptors = {}
sender_to_acceptors = {}

leader_thread = None
scout_threads = {}
scout_conditions = {}
commander_threads = {}
commander_conditions = {}
scout_response = {}
commander_response = {}

adopted_msgs = []
adopted_lock = Lock() 
preempted_msgs = []
preempted_lock = Lock()

ballot_num = 0 
active = False 
proposals = set() 


def append_to_list(lst, lock, msg): 
	lock.acquire() 
	lst.append(msgs)
	lock.release()

def send_adopted_to_leader(b, pvalues): 
	append_to_list(adopted_msgs, adopted_lock, (b, pvalues)) 

def send_preempted_to_leader(b): 
	append_to_list(preempted_msgs, preempted_lock, b) 

def not_empty(l, lock):
	lock.acquire()
	length = len(l)
	lock.release()
	return (length != 0)


def init_leader_listeners(lid, num_servers):
	global listener_to_replicas, listener_to_acceptors 
	for i in range(num_servers):
		listener_to_replicas[i] = LeaderListenerToReplica(lid, i, num_servers)
		listener_to_replicas[i].start()
	for i in range(num_servers):
		listener_to_acceptors[i] = LeaderListenerToAcceptor(lid, i, num_servers)
		listener_to_acceptors[i].start()


def init_leader_senders(lid, num_servers):
	global sender_to_replicas, sender_to_acceptors
	for i in range(num_servers):
		sender_to_replicas[i] = LeaderSenderToReplica(lid, i, num_servers)
		sender_to_replicas[i].start()
	for i in range(num_servers):
		sender_to_acceptors[i] = LeaderSenderToAcceptor(lid, i, num_servers)
		sender_to_acceptors[i].start()


def init_leader(lid, num_servers):
	global leader_thread
	leader_thread = Thread(target=leader,args=(lid, num_servers))
	leader_thread.start()


def leader(lid, num_servers):
	global scout_threads, NUM_SERVERS, scout_conditions, ballot_num
	global adopted_lock, adopted_msgs, preempted_lock, preempted_msgs, active
	NUM_SERVERS = num_servers	
	cv = Condition()
	scout_thread = Thread(target=Scout, args=(ballot_num, cv))
	scout_thread.start()
	scout_threads[ballot_num] = scout_thread
	scout_conditions[ballot_num] = cv
	while True:
		if(not_empty(adopted_msgs, adopted_lock)):
			adopted_lock.acquire()
			for m in adopted_msgs:
				b, pvals = m
				b = int(b)
				pmax_dictionary = {} 
				for pvalue in pvals:
					b_first, s, p = pvals.split()
					b_first = int(b_first)
					s = int(s)
					if s not in pmax: 
						pmax[s] = b_first, s, p 
					else:
						b_prime, s_prime, p_prime = pmax[s]
						if b_prime < b_first: 
							pmax[s] = b_first, s, p
				pmax = [(s, p) for (b, s, p) in pmax_dictionary.values()]
				new_proposals = set(pmax)
				for (s, p) in proposals: 
					found = False 
					for (s_prime, p_prime) in pmax: 
						if s == s_prime and p_prime != p: 
							found = True 
							break 
					if not found: 
						new_proposals.add((s, p))
				proposals = new_proposals
				for (s, p) in proposals: 
					cv = commander_conditions.get((b,s), Condition())
					commander_conditions[(b,s)] = cv
					newc = Thread(target=Commander, args=(b, s, p, cv))
					commander_threads[(b, s)] = newc
					active = True
					newc.start()
		elif(not_empty(preempted_msgs, preempted_lock)):
			preempted_lock.acquire()
			for b in preempted_msgs:
				if b > ballot_num:
					active = False
					ballot_num = (b/NUM_SERVERS + 1)*NUM_SERVERS + lid
					cv = scout_conditions.get(ballot_num, Condition())
					scout_conditions[ballot_num] = cv
					scout_thread = Thread(target=Scout, args=(ballot_num, cv))
					scout_thread.start()
					scout_threads[ballot_num] = scout_thread
			preempted_msgs = []
			preempted_lock.release()
		else:
			time.sleep(SLEEP)


def Scout(b, cv):
	global NUM_SERVERS, scout_response, sender_to_acceptors
	waitfor = set(listener_to_acceptors.keys()) 
	pvalues = set() 
	for i in sender_to_acceptors: 
		sender_to_acceptors[i].send("p1a " + str(b))
	pvalues = set()
	while True:
		with cv:
			while (not scout_response.get(b, [])):
				cv.wait()
			responses = scout_response[b]
			for r in responses:
				aid, b_num, p_vals = r
				if b_num == b:
					waitfor.remove(aid)
					pvalues.update(p_vals)
					if len(waitfor < NUM_SERVERS/2):
						entry = b, pvalues
						send_adopted_to_leader(entry)
						scout_response[b] = []
						return
				else:
					scout_response[b] = []
					send_preempted_to_leader(b_num)
					return
			scout_response[b] = []


def Commander(b, s, p, cv):
	global commander_response
	waitfor = set(listener_to_acceptors.keys()) 
	for i in sender_to_acceptors: 
		sender_to_acceptors[i].send("p2a {} {} {}".format(str(b), str(s), str(p)))
	while True:
		with cv:
			while (not commander_response.get(b, [])):
				cv.wait()
			responses = commander_response[b]
			for r in responses:
				aid, b_num = r
				if b_num == b:
					waitfor.remove(aid)
					if len(waitfor < NUM_SERVERS/2):
						for i in LeaderSenderToReplica:
							LeaderSenderToReplica[i].send("decision {} {}".format(str(s), str(p)))
						commander_response[b] = []
						return
				else:
					commander_response[b] = []
					send_preempted_to_leader(b_num)
					return
			commander_response[b] = []


class LeaderListenerToReplica(Thread): 
	def __init__(self, lid, rid, num_servers): 
		Thread.__init__(self)
		self.lid = lid 
		self.rid = rid 
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		baseport = LEADER_BASEPORT + 4 * lid * num_servers
		self.port = baseport + self.rid 
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self):
		global commander_threads, commander_conditions, ballot_num
		self.conn, self.addr = self.sock.accept()
		_ , port = self.addr
		print "leader " + str(self.lid) + " listen to replica "  + str(self.rid) + ' with port ' + str(port) + " at port " + str(self.port)
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
							cv = Condition()
							newc = Thread(target=Commander, args=(ballot_num, s, p, cv))
							commander_threads[(ballot_num, s)] = newc
							commander_conditions[(ballot_num,s)] = cv
							newc.start()
				else: 
					print "invalid command in ReplicaListenerToLeader"
			else: 
				try: 
					data = self.conn.recv(1024)
					if data == "": 
						raise ValueError
					self.buffer += data 
				except Exception as e:
					print str(self.lid) + " to " + str(self.rid) + " connection closed"
					self.conn.close()
					self.conn = None 
					self.conn, self.addr = self.sock.accept()


class LeaderSenderToReplica(Thread): 
	def __init__(self, lid, rid, num_servers): 
		Thread.__init__(self)
		self.lid = lid 
		self.rid = rid
		self.target_port = BASEPORT + 2 * rid * num_servers + lid 
		self.port = LEADER_BASEPORT + 4 * lid * num_servers + num_servers + rid
		self.connected = False

	def run(self):
		while not self.connected:
			try:
				new_socket = socket(AF_INET, SOCK_STREAM)
				new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				new_socket.bind((ADDR, self.port))
				new_socket.connect((ADDR, self.target_port))
				self.sock = new_socket
				self.connected = True
				print "leader " + str(lid) + " send to replica " + str(rid) + " at port " + str(self.target_port) + " from " + str(self.port)
			except:
				time.sleep(SLEEP)

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
				new_socket.bind((ADDR, self.port))
				new_socket.connect((ADDR, self.target_port))
				self.sock = new_socket
			except: 
				time.sleep(SLEEP)


class LeaderListenerToAcceptor(Thread): 
	def __init__(self, lid, aid, num_servers): 
		Thread.__init__(self)
		self.lid = lid
		self.aid = aid
		self.num_accpeters = num_servers
		self.port = LEADER_BASEPORT + 4 * lid * num_servers + 2 * num_servers + aid
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''


	def run(self): 
		global scout_response, commander_response, scout_conditions, commander_conditions
		self.conn, self.addr = self.sock.accept()	
		# addr, port = self.addr	
		# print "leader " + str(self.lid) + " listen to acceptor " + str(self.aid) + " \nConnection: address = " + str(addr) + " port = " + str(port)
		print "leader " + str(self.lid) + " listen to acceptor " + str(self.aid) + " at port " + str(self.port)
		while True:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				cmd, info = l.split(None, 1)
				if (cmd == 'p1b'):
					proposed_b, b_num, accepts = info.split(None, 2)
					proposed_b = int(proposed_b)
					b_num = int(b_num)
					pvalues = accepts.strip().split(';')
					cv = scout_conditions[proposed_b]
					with cv:
						entry = self.aid, b_num, pvalues
						rlist = scout_response.get(proposed_b, [])
						rlist.append(entry)
						scout_response[proposed_b] = rlist
						cv.notify()

				elif (cmd == 'p2b'):
					proposed_b, b_num = info.split()
					proposed_b = int(proposed_b)
					b_num = int(b_num)
					cv = commander_conditions[proposed_b]
					with cv:
						entry = self.aid, b_num
						rlist = commander_response.get(proposed_b, [])
						rlist.append(entry)
						commander_response[proposed_b] = rlist
						cv.notify() 
				else:
					print "Unknown command {}".format(l)
			else:
				try:
					data = self.conn.recv(1024)
					if data == "":
						raise ValueError
					self.buffer += data 
				except Exception as e:
					print "Leader " + str(self.lid) + " lose connection to acceptor " + str(self.aid)
					self.conn.close()
					self.conn = None 
					self.conn, self.addr = self.sock.accept()


class LeaderSenderToAcceptor(Thread): 
	def __init__(self, lid, aid, num_servers): 
		Thread.__init__(self)
		self.lid = lid
		self.aid = aid
		self.num_accpeters = num_servers
		self.target_port = ACCEPTER_BASEPORT + 2 * aid * num_servers + lid
		self.port = LEADER_BASEPORT + 4 * lid * num_servers + 3 * num_servers + aid
		self.connected = False

	def run(self): 
		while not self.connected:
			try:
				new_socket = socket(AF_INET, SOCK_STREAM)
				new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				new_socket.bind((ADDR, self.port))
				new_socket.connect((ADDR, self.target_port))
				self.sock = new_socket
				self.connected = True
				print "leader " + str(lid) + " send to acceptor " + str(aid) + " at port " + str(self.target_port) + " from " + str(self.port)
			except:
				time.sleep(SLEEP)

	def send(self, msg): 
		if not msg.endswith('\n'):
			msg += '\n'
		try:
			self.sock.send(msg)
		except Exception as e:
			if self.sock:
				self.sock.close()
  				self.sock = None
	  		try:
	  			new_socket = socket(AF_INET, SOCK_STREAM)
				new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				new_socket.bind((ADDR, self.port))
				new_socket.connect((ADDR, self.target_port))
				self.sock = new_socket
			except:
				time.sleep(SLEEP) 