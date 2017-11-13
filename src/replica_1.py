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
	def __init__(self, server, pid, num_servers):
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.server = server
		print "replica " + str(pid) + " at port " + str(port)
		self.buffer = ""
		self.slot_number = 1 
		self.proposals = set() 
		self.decisions = set()

	def run(self):
		pass

	def propose(self, p): 
		found = False 
		for t in self.decisions: 
			(s, p_prime) = t 
			if p == p_prime:
				found = True 
				break
		print "not found a decision for proposal " + p
		if not found: 
			total_set = self.decisions.union(self.proposals)
			all_slots_taken = [s for (s, p) in total_set]
			print "current slots"
			print all_slots_taken
			if len(all_slots_taken) == 0: 
				upper_bound = 2
			else:
				upper_bound = max(all_slots_taken) + 2 
			s_prime = -1 
			for i in range (1, upper_bound): 
				if i not in all_slots_taken: 
					s_prime = i
					break 
			self.proposals.add((s_prime, p))
			for i in replica_senders_to_leaders:
				replica_senders_to_leaders[i].send("propose " + str(s_prime) + " " + p + "\n")






	def run(self):
		global state 
		while self.connected:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				(cmd, arguments) = l.split(" ", 1)
				print "Replica {:d} receives msgs from master {}".format(self.pid, l)
				if cmd == "get":
					self.master_conn.send('chatLog {}\n'.format(state.toString()))
				elif cmd == "msg": 
					print "Replica {:d} makes proposal".format(self.pid)
					self.propose(arguments) 
				elif cmd == "crash":
					exit()
				elif cmd == "crashAfterP1b":
					broadcast_to_leaders('crashafterP1b')
				elif cmd == "crashAfterP2b":
					broadcast_to_leaders('crashafterP2b')
				elif cmd == "crashP1a" or cmd == "crashP2a" or cmd == "crashDecision":
					# Crash after sending p1a
					broadcast_to_leaders(l)
				else:
					print "Unknown command {}".format(l)
			elif len_decision_msgs() != 0: 
				arguments = pop_head_decision_msgs() 
				s, p = arguments.split(" ", 1)
				self.decisions.add((s, p))
				while True: 
					pair = None 
					for t in self.decisions: 
						s1, p1 = t 
						if s1 == self.slot_number: 
							pair = (s1, p1) 
							break
					if pair == None: 
						break
					s1, p1 = pair 
					for t in self.proposals: 
						s2, p2 = t 
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
					self.master_conn.close()
					self.master_conn = None 
					self.master_conn, self.master_addr = self.socket.accept()

	
	def perform(p): 
		cid, msg = p.split(" ", 1)
		found = False 
		for i in range(self.slot_number): 
			if t in self.decisions: 
				(i, p) = t
				found = True 
		if found: 
			self.slot_number += 1 
		else: 
			result = self.state.op(msg) 
			self.slot_number += 1 
			self.master_conn.send("ack " + str(cid) + " " + str(result) + "\n")

	def kill(self):
		try:
			self.connected = False
			self.master_conn.close()
			self.socket.close()
		except:
			pass


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

	def kill(self):
		try:
			self.conn.close()
			self.sock.close()
		except:
			pass

class ReplicaSenderToLeader(Thread): 
	def __init__(self, rid, lid, num_servers): 
		Thread.__init__(self)
		self.rid = rid 
		self.lid = lid
		self.target_port = LEADER_BASEPORT + 4 * lid * num_servers + rid 
		self.port = BASEPORT + 2 * rid * num_servers + num_servers + lid
		# print "replica with port " +  str(self.port) + " connecting to leader " + str(self.lid) + " at port " + str(self.target_port)
		self.connected = False
		self.sock = None 

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
		print "Replica {:d} sends to leader {:d} at port {:d}: {}".format(self.rid, self.lid, self.target_port, msg)

	def kill(self):
		try:
			self.sock.close()
		except:
			pass

def broadcast_to_leaders(msg):
	global replica_senders_to_leaders
	for i in replica_senders_to_leaders:
		replica_senders_to_leaders[i].send(msg)


def exit():
	global replica_listeners_to_leaders, replica_senders_to_leaders
	for i in replica_listeners_to_leaders:
		replica_listeners_to_leaders[i].kill()
	for i in replica_senders_to_leaders:
		replica_senders_to_leaders[i].kill()

def main(pid, num_servers, port):
	# print "starting main"
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
