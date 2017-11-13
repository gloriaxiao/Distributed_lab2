#!/usr/bin/env python
import acceptor
import leader
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
from threading import Thread, Lock

TIMEOUT = 0.2
SLEEP = 0.05
ADDR = 'localhost'
MAXPORT = 22499
BASEPORT = 20000
LEADER_BASEPORT = 25000

replica_listeners_to_leaders = {}
replica_senders_to_leaders = {}
decision_msgs = [] 
decision_lock = Lock()

def append_decision_msgs(msg): 
	decision_lock.acquire()
	decision_msgs.append(msg)
	decision_lock.release()

def pop_head_decision_msgs(): 
	decision_lock.acquire()
	head = decision_msgs[0]
	decision_msgs = decision_msgs[1:]
	decision_lock.release()
	return head 

def len_decision_msgs(): 
	decision_lock.acquire()
	length = len(decision_msgs)
	decision_lock.release()
	return length

class State: 
	def __init__(self): 
		self.count = 0 
		self.mandatory = [] 
		self.optional = [] 

	def op(self, msg): 
		self.mandatory.append(msg) 
		return len(self.mandatory)

	def toString(self): 
		return ("{" + "'count': " + str(self.count) + 
			", mandatory: " + ",".join(self.mandatory) + 
			", optional: " + ",".join(self.optional))

state = State() 

class Replica(Thread):
	def __init__(self, pid, num_servers, port):
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.port = port
		print "replica " + str(pid) + " at port " + str(port)
		self.buffer = ""
		for i in range(self.num_servers):
			replica_listeners_to_leaders[i] = ReplicaListenerToLeader(pid, i, num_servers)
			replica_listeners_to_leaders[i].start()
		leader.init_leader_listeners(self.pid, num_servers)
		acceptor.init_acceptor_listeners(self.pid, num_servers)
		for i in range(self.num_servers): 
			replica_senders_to_leaders[i] = ReplicaSenderToLeader(pid, i, num_servers) 
			replica_senders_to_leaders[i].start()
		leader.init_leader_senders(self.pid, num_servers)
		acceptor.init_acceptor_senders(self.pid, num_servers)
		self.slot_number = 1 
		self.proposals = set() 
		self.decisions = set()
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True

	def run(self):
		global state 
		while self.connected:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				(cmd, arguments) = l.split(" ", 1)
				print "Replica {:d} receives msgs from master {}".format(self.pid, l)
				if cmd == "get":
					self.master_conn.send('chatLog {}\n'.format(state.toString()))
				elif cmd == "msg": 
					self.propose(arguments) 
				elif cmd == "crash":
					pass
				elif cmd == "crashAfterP1b":
					pass
				elif cmd == "crashAfterP2b":
					pass
				elif cmd == "crashP1a":
					# Crash after sending p1a
					pids = [int(i) for i in l.split()[1:]]
					pass
				elif cmd == "crashP2a":
					pids = [int(i) for i in l.split()[1:]]
					pass
				elif cmd == "crashDecision":
					pids = [int(i) for i in l.split()[1:]]
					pass
				else:
					print "Unknown command {}".format(l)
			elif len_decision_msgs() != 0: 
				arguments = pop_head_decision_msgs() 
				s, p = arguments.split(" ", 1)
				self.decisions.union(set((s, p)))
				while True: 
					pair = None 
					for (s1, p1) in self.decisions: 
						if s1 == self.slot_number: 
							pair = (s1, p1) 
							break
					if pair == None: 
						break
					s1, p1 = pair 
					for (s2, p2) in self.proposals: 
						if s2 == self.slot_number and p1 != p2: 
							self.propose(p2)
							break 
					self.perform(p1)
			else:
				try:
					data = self.master_conn.recv(1024)
					if data == "":
						raise ValueError
					self.buffer += data 
				except Exception as e:
					print "Leader " + str(self.lid) + " lose connection to acceptor " + str(self.aid)
					self.master_conn.close()
					self.master_conn = None 
					self.master_conn, self.master_addr = self.socket.accept()

	def propose(self, p): 
		found = False 
		for (s, p_prime) in self.decisions: 
			if p == p_prime:
				found = True 
				break 
		if not found: 
			total_set = self.decisions.union(self.proposals)
			all_slots_taken = [s for (s, p) in total_set]
			s_prime = -1 
			for i in range (1, max(all_slots_taken) + 2): 
				if i not in all_slots_taken: 
					s_prime = i
					break 
			self.proposals.union(set((s_prime, p)))
			for leader in replica_senders_to_leaders: 
				leader.send("propose " + str(s_prime) + " " + p + "\n")

	def perform(p): 
		cid, msg = p.split(" ", 1)
		found = False 
		for i in range(self.slot_number): 
			if (i, p) in self.decisions: 
				found = True 
		if found: 
			self.slot_number += 1 
		else: 
			result = self.state.op(msg) 
			self.slot_number += 1 
			self.master_conn.send("ack " + str(cid) + " " + str(result) + "\n")


class ReplicaListenerToLeader(Thread): 
	def __init__(self, rid, lid, num_servers): 
		Thread.__init__(self)
		self.rid = rid
		self.lid = lid 
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = BASEPORT + 2 * rid * num_servers + lid 
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self): 
		self.conn, self.addr = self.sock.accept()
		# print "replica " + str(self.rid) + " listen to leader " + str(self.lid) + " at port " + str(self.port)
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest 
				cmd, arguments = l.split(" ", 1)
				if cmd == "decision": 
					append_decision_msgs(arguments)
				else: 
					print "invalid command in ReplicaListenerToLeader"
			else: 
				try:
					data = self.conn.recv(1024)
					if data == "":
						raise ValueError
					self.buffer += data 
				except Exception as e:
					print 'replica ' + str(self.rid) + " to leader " + str(self.lid) + " connection closed"
					self.conn.close()
					self.conn = None 
					self.conn, self.addr = self.sock.accept()


class ReplicaSenderToLeader(Thread): 
	def __init__(self, rid, lid, num_servers): 
		Thread.__init__(self)
		self.rid = rid 
		self.lid = lid
		self.target_port = LEADER_BASEPORT + 4 * lid * num_servers + rid 
		self.port = BASEPORT + 2 * rid * num_servers + num_servers + lid
		# print "replica with port " +  str(self.port) + " connecting to leader " + str(self.lid) + " at port " + str(self.target_port)
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
				# print "replica " + str(self.rid) + " send to leader " + str(self.lid) + " at port " + str(self.target_port) + " from " + str(self.port)
			except Exception as e:
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

def main(pid, num_servers, port):
	print "starting main"
	replica = Replica(pid, num_servers, port)
	replica.start()


if __name__ == "__main__":
	args = sys.argv
	if len(args) != 4:
		print "Need three arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)
