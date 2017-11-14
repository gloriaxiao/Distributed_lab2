#!/usr/bin/env python
import sys
import time
from threading import Lock, Thread, Condition


SLEEP = 0.05

scout_responses = {}
scout_condition = Condition()
commander_threads = {}
commander_conditions = {}
commander_responses = {}

adopted_msg = None
preempted_ballot = set()
preempted_cv = Condition()


def has_preempted():
	global preempted_ballot, preempted_cv
	preempted_cv.acquire()
	length = len(preempted_ballot)
	preempted_cv.release()
	return (length != 0)

def add_preempted(b):
	global preempted_cv, preempted_ballot
	preempted_cv.acquire()
	preempted_ballot.add(b)
	preempted_cv.release()


def Scout(b, pid, num_servers, clients):
	global scout_responses, scout_condition, adopted_msg
	print "Leader {:d} spawn Scout for ballot number {:d}".format(pid, b)
	# waitfor = set(range(0, num_servers)) - set([pid])
	waitfor = set(range(0, num_servers))
	for i in clients:
		clients[i].send("p1a " + str(b))
	pvalues = set()
	while True:
		with scout_condition:
			while not scout_responses:
				scout_condition.wait()
			# print "Leader " + str(pid) + " stopped waiting for scout response"
			for aid, r in scout_responses.items():
				b_num, p_vals = r
				# print "aid: " + str(aid) + " b_num: " + str(b_num) + " pvals: " + str(p_vals)
				if b_num == b:
					waitfor.remove(aid)
					pvalues.update(p_vals)
					if len(waitfor) < num_servers/2:
						print "Leader {:d} gets majority accepted for {:d}".format(pid, b)
						entry = b, pvalues
						adopted_msg = entry
						scout_responses = {}
						return
				else:
					scout_responses = {}
					add_preempted(b_num)
					return
			scout_responses = {}


def Commander(b, s, p, pid, num_servers, clients):
	global commander_responses, commander_conditions
	print "Leader {:d} spawn out a Commander for {:d} {:d} {}".format(pid, b, s, p)
	# waitfor = set(range(0, num_servers)) - set([pid])
	waitfor = set(range(0, num_servers))
	for i in clients: 
		clients[i].send("p2a {} {} {}".format(str(b), str(s), str(p)))
	cv = commander_conditions[(b,s)]
	while True:
		with cv:
			while (not commander_responses.get((b,s), [])):
				cv.wait()
			responses = commander_responses[(b,s)]
			# print "Leader " + str(pid) + " stopped waiting for commander responses"
			for r in responses:
				aid, b_num = r
				# print "leader: " + str(pid) + " aid: " + str(aid) + " b_num: " + str(b_num) + " b: " + str(b)
				if b_num == b:
					waitfor.remove(aid)
					# print "remove " + str(aid) + " from waitfor so length of waitfor is " + str(len(waitfor))
					if len(waitfor) < num_servers/2:
						print "Leader {:d} sends decisions to all replicas".format(pid)
						for i in clients:
							clients[i].send("decision {} {}".format(str(s), str(p)))
						commander_responses[(b,s)] = []
						return
				else:
					commander_responses[(b,s)] = []
					add_preempted(b_num)
					return
			commander_responses[(b,s)] = []

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
		self.commander_threads = {}
		self.scout_thread = None

	def run(self):
		global commander_conditions, commander_responses, adopted_msg
		global preempted_ballot, preempted_cv
		self.scout_thread = Thread(target=Scout, 
							args=(self.ballot_num, self.pid, self.num_servers, self.clients))
		self.scout_thread.start()
		while True:
			if(adopted_msg):
				# print "Leader {:d} gets adopt msg: {}".format(self.pid, adopted_msg)
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
				self.p_lock.acquire()
				for t in self.proposals: 
					s, p = t
					s = int(s)
					found = False 
					for (s_prime, p_prime) in pmax: 
						if s == s_prime and p_prime != p: 
							found = True 
							break 
					if not found: 
						new_proposals.add((s, p))
				self.proposals = new_proposals
				for t in self.proposals: 
					s, p = t
					s = int(s)
					cv = commander_conditions.get((b,s), Condition())
					commander_conditions[(b,s)] = cv
					newc = Thread(target=Commander, args=(b, s, p, self.pid, self.num_servers, self.clients))
					self.commander_threads[(b, s)] = newc
					newc.start()
				self.p_lock.release()
				self.active = True
			elif(has_preempted()):
				with preempted_cv:
					for b in preempted_ballot:
						if b > self.ballot_num:
							active = False
							new_b = (b/self.num_servers + 1)*self.num_servers + self.pid
							self.ballot_num = new_b
							self.scout_thread = Thread(target=Scout, 
												args=(new_b, self.pid, self.num_servers, self.clients))
							self.scout_thread.start()
					preempted_ballot = set()
			else:
				time.sleep(SLEEP)

	def add_proposal(self, proposal):
		global commander_threads, commander_conditions
		s, p = proposal
		s = int(s)
		print "Leader {:d} get proposal {:d}, {}".format(self.pid, s, p)
		found = False
		self.p_lock.acquire()
		for t in self.proposals:
			s_prime, p_prime = t
			if s == s_prime: 
				found = True 
				break
		if not found: 
			self.proposals.add((s, p))
			# print "current proposals"
			# print self.proposals
			self.p_lock.release()
			if self.active:
				# print "system is active at leader {:d}".format(self.pid)
				cv = Condition()
				newc = Thread(target=Commander, args=(self.ballot_num, s, p, cv))
				commander_threads[(self.ballot_num, s)] = newc
				commander_conditions[(self.ballot_num,s)] = cv
				newc.start()
		else:
			self.p_lock.release()

	def process_p1b(self, target_pid, info):
		global scout_condition, scout_responses
		proposed_b, b_num, accepts = info.split(None, 2)
		# print "Leader {:d} receives p1b from Accpetor {:d} with {}".format(self.pid, target_pid, info) 
		proposed_b = int(proposed_b)
		b_num = int(b_num)
		if accepts == "none":
			pvalues = []
		else:
			pvalues = accepts.strip().split(';')
		with scout_condition:
			entry = b_num, pvalues
			# print "Leader {:d} updates its scout response".format(self.pid)
			scout_responses[target_pid] = entry
			scout_condition.notify()


	def process_p2b(self, target_pid, info):
		global commander_conditions, commander_responses
		proposed_b, proposed_s, b_num = info.split()
		proposed_b = int(proposed_b)
		proposed_s = int(proposed_s)
		b_num = int(b_num)
		key = proposed_b, proposed_s
		# print "commander condition keys"
		# print " ".join([str(k[0]) + ', ' + str(k[1]) for k in commander_conditions])
		cv = commander_conditions[key]
		with cv:
			entry = target_pid, b_num
			rlist = commander_responses.get(key, [])
			rlist.append(entry)
			commander_responses[key] = rlist
			cv.notify()
