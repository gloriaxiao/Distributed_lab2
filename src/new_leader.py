#!/usr/bin/env python
import sys
import time
from threading import Lock, Thread, Condition


SLEEP = 0.05
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


def scout(b, pid, num_servers, clients):
	print "Leader {:d} spawn Scout for ballot number {:d}".format(LID, b)
	waitfor = set(range(0, num_servers)) - set([pid])
	for i in clients: 
		clients[i].send("p1a " + str(b))
	pvalues = set()
	while True:
		with scout_condition:
			while not scout_responses:
				scout_condition.wait()
			print "Leader " + str(LID) + " stopped waiting for scout response"
			for aid, r in scout_responses.items():
				b_num, p_vals = r
				print "aid: " + str(aid) + " b_num: " + str(b_num) + " pvals: " + str(p_vals)
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


class Leader(Thread):
	def __init__(self, pid, num_servers, clients):
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.active = False
		self.clients = clients
		self.proposals = set()
		self.p_lock = Lock()
		self.ballot_num = pid
		self.adopted_msgs = None
		self.scout_thread = None
		self.scout_condition = None

	def run(self):
		scout_condition = Condition()
		scout_thread = Thread(target=Scout, args=(ballot_num,))
		scout_thread.start()
		pass

	def add_proposal(proposal):
		s, p = proposal.split(None, 1)
		s = int(s)
		print "Leader {:d} get proposal {}".format(self.pid, p)
		found = False
		self.p_lock.acquire()
		for t in self.proposals:
			s_prime, p_prime = t
			if s == s_prime: 
				found = True 
				break
		if not found: 
			self.proposals.add((s, p))
			print "current proposals"
			print self.proposals
			self.p_lock.release()
			if self.active:
				print "system is active at leader {:d}".format(self.pid)
				cv = Condition()
				newc = Thread(target=Commander, args=(ballot_num, s, p, cv))
				commander_threads[(ballot_num, s)] = newc
				commander_conditions[(ballot_num,s)] = cv
				newc.start()
		else:
			self.p_lock.release()


	def process_p1b(self, ):
		

	def process_p2b(self, ):





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
				print "Leader {:d} gets preempted msg: {}".format(LID, b)
				if b > ballot_num:
					active = False
					ballot_num = (b/NUM_SERVERS + 1)*NUM_SERVERS + lid
					scout_thread = Thread(target=Scout, args=(ballot_num,))
					scout_thread.start()
			preempted_msgs = []
			preempted_lock.release()
		else:
			time.sleep(SLEEP)





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
			print "Leader " + str(LID) + " stopped waiting for commander responses"
			for r in responses:
				aid, b_num = r
				print "leader: " + str(LID) + " aid: " + str(aid) + " b_num: " + str(b_num) + " b: " + str(b)
				if b_num == b:
					waitfor.remove(aid)
					print "remove " + str(aid) + " from waitfor so length of waitfor is " + str(len(waitfor))
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
		self.sock = None

	def run(self): 
		pass
		while not self.sock:
			try:
				new_socket = socket(AF_INET, SOCK_STREAM)
				new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				new_socket.bind((ADDR, self.port))
				new_socket.connect((ADDR, self.target_port))
				self.sock = new_socket
				self.connected = True
				print "leader " + str(self.lid) + " connected to acceptor " + str(self.aid) + " at port " + str(self.target_port) + " from " + str(self.port)
			except:
				time.sleep(SLEEP)

	def send(self, msg): 
		if not msg.endswith('\n'):
			msg += '\n'
		try:
			self.sock.send(msg)
			print "leader " + str(self.lid) + " sent " + msg[:-1] + " to acceptor " + str(self.aid)
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
  			print "leader " + str(self.lid) + " sent " + msg[:-1] + " to acceptor " + str(self.aid) 
  		except:
  			print "leader " + str(self.lid) + " failed to send to acceptor " + str(self.aid) 
  			time.sleep(SLEEP)
		# print "Leader {:d} sends {} to Acceptor {:d}".format(self.lid, msg[:-1], self.aid)