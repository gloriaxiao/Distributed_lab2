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
LID = -1
listener_to_replicas = {}
sender_to_replicas = {}
listener_to_acceptors = {}
sender_to_acceptors = {}

leader_thread = None
scout_thread = None
scout_condition = None
scout_responses = {}

commander_threads = {}
commander_conditions = {}
commander_response = {}

adopted_msg = None
preempted_msgs = []
preempted_lock = Lock()

ballot_num = 0 
active = False 
proposals = set()
proposals_lock = Lock()


def append_to_list(lst, lock, msg): 
	lock.acquire() 
	lst.append(msg)
	lock.release()

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
	global scout_thread, NUM_SERVERS, scout_condition, ballot_num, proposals
	global adopted_msg, preempted_lock, preempted_msgs, active, LID
	global commander_threads, commander_conditions, proposals_lock
	NUM_SERVERS = num_servers
	LID = lid
	scout_condition = Condition()
	ballot_num = lid + ballot_num * num_servers
	scout_thread = Thread(target=Scout, args=(ballot_num,))
	scout_thread.start()
	while True:
		if(adopted_msg):
			print "Leader {:d} gets adopt msg: {}".format(LID, adopted_msg)
			b, pvals = adopted_msg
			adopted_msg = None
			b = int(b)
			pmax_dictionary = {} 
			for pvalue in pvals:
				b_first, s, p = pvalue.split()
				b_first = int(b_first)
				s = int(s)
				if s not in pmax_dictionary: 
					pmax_dictionary[s] = b_first, s, p 
				else:
					b_prime, s_prime, p_prime = pmax_dictionary[s]
					if b_prime < b_first: 
						pmax_dictionary[s] = b_first, s, p
			pmax = [(s, p) for (b, s, p) in pmax_dictionary.values()]
			new_proposals = set(pmax)
			proposals_lock.acquire()
			for t in proposals: 
				s, p = t
				s = int(s)
				found = False 
				for (s_prime, p_prime) in pmax: 
					if s == s_prime and p_prime != p: 
						found = True 
						break 
				if not found: 
					new_proposals.add((s, p))
			proposals = new_proposals
			for t in proposals: 
				s, p = t
				s = int(s)
				cv = commander_conditions.get((b,s), Condition())
				commander_conditions[(b,s)] = cv
				newc = Thread(target=Commander, args=(b, s, p, cv))
				commander_threads[(b, s)] = newc
				newc.start()
			proposals_lock.release()
			active = True
		elif(not_empty(preempted_msgs, preempted_lock)):
			preempted_lock.acquire()
			for b in preempted_msgs:
				if b > ballot_num:
					active = False
					ballot_num = (b/NUM_SERVERS + 1)*NUM_SERVERS + lid
					scout_thread = Thread(target=Scout, args=(ballot_num,))
					scout_thread.start()
			preempted_msgs = []
			preempted_lock.release()
		else:
			time.sleep(SLEEP)


def Scout(b):
	global NUM_SERVERS, scout_responses, sender_to_acceptors, scout_condition
	global adopted_msg, LID
	print "Leader {:d} spawn Scout for ballot number {:d}".format(LID, b)
	waitfor = set(listener_to_acceptors.keys()) 
	for i in sender_to_acceptors: 
		sender_to_acceptors[i].send("p1a " + str(b))
	pvalues = set()
	while True:
		with scout_condition:
			while not scout_responses:
				scout_condition.wait()
			print "Stopped waiting for scout response"
			for aid, r in scout_responses.items():
				b_num, p_vals = r
				if b_num == b:
					waitfor.remove(aid)
					pvalues.update(p_vals)
					if len(waitfor) < NUM_SERVERS/2:
						entry = b, pvalues
						adopted_msg = entry
						scout_responses = {}
						return
				else:
					scout_responses = {}
					send_preempted_to_leader(b_num)
					return
			scout_responses = {}


def Commander(b, s, p, cv):
	global commander_response, LID, sender_to_replicas
	print "Leader {:d} spawn out a Commander for {:d} {:d} {}".format(LID, b, s, p)
	waitfor = set(listener_to_acceptors.keys()) 
	for i in sender_to_acceptors: 
		sender_to_acceptors[i].send("p2a {} {} {}".format(str(b), str(s), str(p)))
	while True:
		with cv:
			while (not commander_response.get((b,s), [])):
				cv.wait()
			responses = commander_response[(b,s)]
			print "Stop waiting for commander responses"
			for r in responses:
				aid, b_num = r
				if b_num == b:
					waitfor.remove(aid)
					if len(waitfor) < NUM_SERVERS/2:
						for i in sender_to_replicas:
							sender_to_replicas[i].send("decision {} {}".format(str(s), str(p)))
						commander_response[(b,s)] = []
						return
				else:
					commander_response[(b,s)] = []
					send_preempted_to_leader(b_num)
					return
			commander_response[(b,s)] = []


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
		global commander_threads, commander_conditions, ballot_num, proposals, proposals_lock
		self.conn, self.addr = self.sock.accept()
		# print "leader " + str(self.lid) + " listen to replica "  + str(self.rid) + ' with port ' + str(port) + " at port " + str(self.port)
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest 
				# print "Leader {:d} at port {:d} gets {} from replica {:d}".format(self.lid, self.port, l, self.rid)
				cmd, arguments = l.split(None, 1)
				if cmd == "propose": 
					s, p = arguments.split(None, 1)
					s = int(s)
					print "Leader {:d} get proposal {}".format(self.lid, p)
					found = False
					proposals_lock.acquire()
					for t in proposals:
						s_prime, p_prime = t
						if s == s_prime: 
							found = True 
							break
					if not found: 
						proposals.add((s, p))
						print "current proposals"
						print proposals
						proposals_lock.release()
						if active:
							print "system is active at leader {:d}".format(self.lid)
							cv = Condition()
							newc = Thread(target=Commander, args=(ballot_num, s, p, cv))
							commander_threads[(ballot_num, s)] = newc
							commander_conditions[(ballot_num,s)] = cv
							newc.start()
					else:
						proposals_lock.release()
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
				# print "leader " + str(self.lid) + " send to replica " + str(self.rid) + " at port " + str(self.target_port) + " from " + str(self.port)
			except Exception as e:
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
				self.sock.send(msg)
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
		global scout_responses, commander_response, scout_condition, commander_conditions
		self.conn, self.addr = self.sock.accept()	
		# print "leader " + str(self.lid) + " listen to acceptor " + str(self.aid) + " at port " + str(self.port)
		while True:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				cmd, info = l.split(None, 1)
				if (cmd == 'p1b'):
					proposed_b, b_num, accepts = info.split(None, 2)
					print "Leader {:d} receives p1b from Accpetor {:d} with {}".format(self.lid, self.aid, info) 
					proposed_b = int(proposed_b)
					b_num = int(b_num)
					if accepts == "none":
						pvalues = []
					else:
						pvalues = accepts.strip().split(';')
					with scout_condition:
						entry = b_num, pvalues
						print "Leader {:d} updates its scout response".format(self.lid)
						scout_responses[self.aid] = entry
						scout_condition.notify()
				elif (cmd == 'p2b'):
					proposed_b, proposed_s, b_num = info.split()
					proposed_b = int(proposed_b)
					proposed_s = int(proposed_s)
					b_num = int(b_num)
					key = (proposed_b, proposed_s)
					cv = commander_conditions[key]
					with cv:
						entry = self.aid, b_num
						rlist = commander_response.get(key, [])
						rlist.append(entry)
						commander_response[key] = rlist
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
		self.sock = None

	def run(self): 
		pass
		# while not self.connected:
		# 	try:
		# 		new_socket = socket(AF_INET, SOCK_STREAM)
		# 		new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		# 		new_socket.bind((ADDR, self.port))
		# 		new_socket.connect((ADDR, self.target_port))
		# 		self.sock = new_socket
		# 		self.connected = True
		# 		print "leader " + str(self.lid) + " connected to acceptor " + str(self.aid) + " at port " + str(self.target_port) + " from " + str(self.port)
		# 	except:
		# 		time.sleep(SLEEP)

	def send(self, msg): 
		if not msg.endswith('\n'):
			msg += '\n'
		try:
			self.sock.send(msg)
		except Exception as e:
			if self.sock:
				self.sock.close()
  				self.sock = None
  			while not self.connected:
		  		try:
		  			new_socket = socket(AF_INET, SOCK_STREAM)
					new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
					new_socket.bind((ADDR, self.port))
					new_socket.connect((ADDR, self.target_port))
					self.sock = new_socket
					self.connected = True
					self.sock.send(msg)
				except:
					time.sleep(SLEEP)
		# print "Leader {:d} sends {} to Acceptor {:d}".format(self.lid, msg[:-1], self.aid)