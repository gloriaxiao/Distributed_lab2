#!/usr/bin/env python
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
import os, errno
from threading import Thread, Condition
from new_leader import Leader
from new_acceptor import Acceptor, Pvalue 

master_thread = None
SLEEP = 0.05
ADDR = 'localhost'
BASEPORT = 20000
chatLog = []
listeners = {}
clients = {}
replica = None 
leader = None
acceptor = None
LOG_PATH_REPLICA = None 
LOG_PATH_LEADER = None 
LOG_PATH_ACCEPTOR = None 
crash = None 
cache = [] 

leader_cv = Condition() 
acceptor_cv = Condition() 
replica_cv = Condition() 

decisions_cv = Condition() 
proposals_cv = Condition() 
chatLog_cv = Condition() 

class Crash:
	def __init__(self):
		self.crashAfterP1b = False 
		self.crashAfterP2b = False 
		self.crashP1a = False  
		self.crashP2a = False
		self.crashDecision = False 
		self.p1a = []
		self.p2a = []
		self.decision = [] 

class State: 
	def __init__(self): 
		self.data = {}

	def op(self, sid, msg): 
		self.data[sid] = msg
		return len(self.data)

	def __str__(self):
		keylist = self.data.keys()
		keylist.sort()
		return ",".join([self.data[k] for k in keylist])

state = State()

def save_to_chatLog(): 
	global chatLog, LOG_PATH_ACCEPTOR, LOG_PATH_LEADER, LOG_PATH_REPLICA, leader, acceptor, state 
	with open(LOG_PATH_REPLICA, 'wt') as replica_file: 
		replica_file.write('{}\n'.format(str(replica.slot_number)))
		replica_file.write('{}\n'.format(str(len(state.data))))
		for key in state.data: 
			replica_file.write(str(key) + " " + state.data[key] + "\n")
		replica_file.write('{}\n'.format(len(chatLog)))
		for line in chatLog: 
			replica_file.write(line + '\n')
		replica_file.write('{}\n'.format(len(replica.proposals)))
		for i in replica.proposals: 
			replica_file.write(str(i) + " " + str(replica.proposals[i]))
		replica_file.write('{}\n'.format(len(replica.decisions)))
		for i in replica.decisions: 
			replica_file.write(str(i) + " " + str(replica.decisions[i]))
	with open(LOG_PATH_LEADER, 'wt') as leader_file: 
		leader_file.write('{}\n'.format(len(leader.proposals))) 
		for line in leader.proposals: 
			s, p = line 
			leader_file.write(str(s) + " " + str(p) + "\n")
	with open(LOG_PATH_ACCEPTOR, 'wt') as acceptor_file: 
		acceptor_file.write('{}\n'.format(len(acceptor.accepted)))
		for line in acceptor.accepted: 
			acceptor_file.write(str(line) + "\n")

def replica_load_from_chatLog(): 
	global chatLog, LOG_PATH_REPLICA, replica_cv, state
	with replica_cv: 
		while replica == None: 
			print "load from chatlog" + " $$$$$$$$$$$$$$$$$$$$$$ grabbed replica lock_cv lock"
			replica_cv.wait() 
	try:
		with open(LOG_PATH_REPLICA, 'rt') as file: 
			replica.slot_number = int(file.readline())
			state_line = int(file.readline())
			for i in range(state_line): 
				line = file.readline() 
				key, value = line.split(None, 1)
				key = int(key)
				state.data[key] = value[:-1]
			chatLog_line = int(file.readline())
			for i in range(chatLog_line): 
				line = file.readline()
				chatLog.append(line)
			print chatLog
			proposal_line = int(file.readline())
			for i in range(proposal_line): 
				line = file.readline()
				key, value = line.split(None, 1)
				key = int(key)
				replica.proposals[key] = value 
			decision_line = int(file.readline())
			for i in range(decision_line): 
				line = file.readline()
				key, value = line.split(None, 1)
				key = int (key)
				replica.decisions[key] = value 
	except IOError as e: 
		pass 

class Replica(Thread):
	def __init__(self, pid, num_servers, port):
		global listeners, clients
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.port = port
		print "replica " + str(pid) + " at port " + str(port)
		self.buffer = ""
		self.slot_number = 1 
		self.proposals = {}
		self.decisions = {}
		for i in range(num_servers):
			listeners[i] = ServerListener(pid, i, num_servers)
			listeners[i].start()
		for i in range(num_servers): 
			clients[i] = ServerClient(pid, i, num_servers) 
			clients[i].start()
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True
		self.leader_initialized = False 

	def run(self):
		global state, crash 
		replica_load_from_chatLog()
		print "####################################loaded from chatlog"
		print "self.decisions is " + str(self.decisions)
		print "self.proposals is " + str(self.proposals)
		while self.connected:
			if '\n' in self.buffer:
				with leader_cv: 
					while leader == None: 
						leader_cv.wait() 
				if not self.leader_initialized: 
					leader.start()
					self.leader_initialized = True 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				try: 
					(cmd, arguments) = l.split(" ", 1)
				except: 
					cmd = l 
				print "Replica {:d} receives msgs from master {}".format(self.pid, l)
				if cmd == "get":
					print str(state)
					self.master_conn.send('chatLog {}\n'.format(str(state)))
				elif cmd == "msg": 
					print "Replica {:d} makes proposal".format(self.pid)
					self.propose(arguments) 
				elif cmd == "crash":
					exit()
				elif cmd == "crashAfterP1b":
					crash.crashAfterP1b = True 
				elif cmd == "crashAfterP2b":
					crash.crashAfterP2b = True 
				elif cmd == "crashP1a" or cmd == "crashP2a" or cmd == "crashDecision":
					lst = arguments.split()
					for i in range(len(lst)):						
						lst[i] = int(lst[i])
					if cmd == "crashP1a": 
						crash.crashP1a = True 
						crash.p1a = lst 
					elif cmd == "crashP2a": 
						crash.crashP2a = True 
						crash.p2a = lst 
					elif cmd == "crashDecision": 
						crash.crashDecision = True 
						crash.decision = lst 
				else:
					print "Unknown command {}".format(l)
			else:
				try:
					data = self.master_conn.recv(1024)
					if data == "":
						raise ValueError
					self.buffer += data 
				except Exception as e:
					print "################################################ connection to master closed"
					self.master_conn.close()
					self.master_conn = None 
					self.master_conn, self.master_addr = self.socket.accept()


	def decide(self, arguments): 
		s, p = arguments.split(" ", 1)
		s = int(s)
		self.decisions[s] = p
		print "Replica: " + str(self.pid) + " self.decisions: " + str(self.decisions) + " " + str(self.slot_number)
		for slot, op in self.decisions.items():
			if slot == self.slot_number: 
 				if slot in self.proposals:
 					propose_p = self.proposals[slot]
 					if propose_p != op:
 						self.propose(propose_p)
 						del self.proposals[slot]
				self.perform(slot, op)


	def propose(self, p):
		global leader, replica_cv
		found = False 
		for s in self.decisions: 
			if p == self.decisions[s]:
				found = True 
				break
		if not found:
			propose_s = 1
			while True:
				if (propose_s in self.decisions) or (propose_s in self.proposals):
					propose_s += 1
				else:
					break
			proposal = propose_s, p
			self.proposals[propose_s] = p
			with replica_cv: 
				while replica == None: 
					print str(self.pid) + "$$$$$$$$$$$$$$$$$$$$$$ grabbed replica lock_cv lock"
					replica_cv.wait() 
			leader.add_proposal(proposal)


	def perform(self, s, p): 
		# print "in perform"
		cid, op = p.split(" ", 1)
		print "Replica {:d} perform on msg {:d}".format(self.pid, int(cid)) + " slot_number: " + str(self.slot_number)
		found = False 
		for s_prime, p_prime in self.decisions.items(): 
			if s_prime < self.slot_number and p == p_prime: 
				found = True 
				break 
		if found:
			self.slot_number += 1 
		else: 
			global state 
			result = state.op(s, op) 
			self.slot_number += 1 
			print "Replica " + str(self.pid) + " state: " + str(state) + " slot_number: " + str(self.slot_number)
			if p in self.proposals.values(): 	
				ack_msg = "ack " + str(cid) + " " + str(result) + "\n"
				print "Replica {:d} sends ACK back to master: {}".format(self.pid, str(ack_msg))
				self.master_conn.send("ack " + str(cid) + " " + str(result) + "\n")


	def kill(self):
		try:
			self.connected = False
			self.master_conn.close()
			self.socket.close()
		except:
			pass


class ServerListener(Thread):
	def __init__(self, pid, target_pid, num_servers): 
		Thread.__init__(self)
		self.pid = pid
		self.target_pid = target_pid
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = 29999 - pid * 100 - target_pid 
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self):
		global leader, acceptor, replica, replica_cv, acceptor_cv, leader_cv, replica_cv
		self.conn, self.addr = self.sock.accept()
		# print "Server " + str(self.pid) + " listen to Server " + str(self.target_pid) + " at port " + str(self.port)
		while True:
			if '\n' in self.buffer:
				l, rest = self.buffer.split('\n', 1)
				self.buffer = rest
				cmd, info = l.split(None, 1)
				# leader receives
				if cmd == 'p1b':
					with leader_cv: 
						while leader == None: 
							print str(self.pid) + " $$$$$$$$$$$$$$$$$$$$$$ grabbed replica lock_cv lock"
							leader_cv.wait() 
					leader.process_p1b(self.target_pid, info)
				elif cmd == 'p2b':
					with leader_cv: 
						while leader == None: 
							print str(self.pid) + " $$$$$$$$$$$$$$$$$$$$$$ grabbed leader lock_cv lock"
							leader_cv.wait() 
					leader.process_p2b(self.target_pid, info)
				# accpetor recieves
				elif cmd == 'p1a':
					with acceptor_cv: 
						while acceptor == None: 
							print str(self.pid) + " $$$$$$$$$$$$$$$$$$$$$$ grabbed acceptor lock_cv lock"
							acceptor_cv.wait() 
					acceptor.process_p1a(self.target_pid, info)
				elif cmd == 'p2a':
					with acceptor_cv: 
						while acceptor == None: 
							print str(self.pid) + " $$$$$$$$$$$$$$$$$$$$$$ grabbed acceptor lock_cv lock"
							acceptor_cv.wait() 
					acceptor.process_p2a(self.target_pid, info)
				elif cmd == 'decision':
					# print "Server " + str(self.pid) + " received decision from leader " + str(self.target_pid)
					with replica_cv: 
						while replica == None: 
							print str(self.pid) + " $$$$$$$$$$$$$$$$$$$$$$ grabbed replica lock_cv lock"
							replica_cv.wait() 
					replica.decide(info)
				elif cmd == 'heartbeat': 
					pass 
				else: 
					print "invalid command in ReplicaListenerToLeader"
			else:
				try:
					data = self.conn.recv(1024)
					if data == "":
						raise ValueError
					self.buffer += data 
				except Exception as e:
					print 'Server ' + str(self.pid) + " to Server " + str(self.target_pid) + " connection closed"
					self.conn.close()
					self.conn = None 
					self.conn, self.addr = self.sock.accept()

	def kill(self):
		try:
			self.conn.close()
		except:
			pass


class ServerClient(Thread):
	def __init__(self, pid, target_pid, num_servers):
	  	Thread.__init__(self)
	  	self.pid = pid
	  	self.target_pid = target_pid 
	  	self.num_servers = num_servers
	  	self.target_port = 29999 - target_pid * 100 - pid
	  	self.port = 29999 - 100 * pid - num_servers - target_pid
	  	self.sock = None

	def run(self):
		global cache 
		while True: 
			try: 
				self.sock.send("heartbeat " + str(self.pid) + "\n") 
			except:
				try:
					self.sock = None
					s = socket(AF_INET, SOCK_STREAM)
					s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
					s.connect((ADDR, self.target_port))
					print "server " + str(self.pid) + " connected to " + str(self.target_pid) + " cache: " + str(cache)
					self.sock = s 
					self.sock.send("heartbeat " + str(self.pid) + "\n")
					for i in cache: 
						# print "sending cache"
						self.sock.send(i)
					cache = [] 
				except: 
					time.sleep(0.1) 

	def send(self, msg): 
		global crash 
		cmd, arguments = msg.split(None, 1)
		if cmd == "p1a" and crash.crashP1a: 
			print "crashing p1a"
			if self.target_port not in crash.p1a: 
				return 
			else: 
				crash.p1a.remove(self.target_port)
				self.forward_msg(msg)
				if len(crash.p1a) == 0: 
					exit()
		elif cmd == "p2a" and crash.crashP2a: 
			print "crashing p2a"
			if self.target_port not in crash.p2a: 
				return 
			else: 
				crash.p2a.remove(self.target_port)
				self.forward_msg(msg)
				if len(crash.p2a) == 0:
					exit()
		elif cmd == "decision" and crash.crashDecision: 
			print "crashing decision"
			if self.target_port not in crash.decision: 
				return 
			else: 
				crash.decision.remove(self.target_port)
				self.forward_msg(msg)
				if len(crash.decision) == 0:
					exit()
		elif cmd == "p1b" and crash.crashAfterP1b: 
			print "crashing p1b"
			self.forward_msg(msg)
			exit() 
		elif cmd == "p2b" and crash.crashAfterP2b: 
			print "crashing p2b"
			self.forward_msg(msg)
			exit()
		else: 
			self.forward_msg(msg)

	def forward_msg(self, msg): 
		global cache 
		if not msg.endswith("\n"): 
			msg = msg + "\n"
		try: 
			self.sock.send(msg)
		except: 
			try: 
				self.sock = None 
				s = socket(AF_INET, SOCK_STREAM)
				s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				s.connect((ADDR, self.target_port))
				print "serverclient " + str(self.pid) + " connected to " + str(self.target_pid) + " cache: " + str(cache)
				self.sock = s 
				for i in cache: 
					self.sock.send(i)
				cache = [] 
				self.sock.send(msg)
			except:
				if not msg.startswith("heartbeat"): 
					cache.append(msg)
					print "serverclient: " + str(self.pid) + " target: " + str(self.target_pid) + " append message: " + msg[:-1] + " cache: " + str(cache)
				print "***************************** " 
				time.sleep(SLEEP)


	def kill(self):
		try:
			self.sock.close()
		except:
			pass


def make_sure_path_exists(path):
	try:
		os.makedirs(path)
	except OSError as e:
		if e.errno != errno.EEXIST:
			print("Error: Path couldn't be recognized!")
			print(e)

def exit():
	print "exit is called"
	global listeners, clients
	for i in listeners:
		listeners[i].kill()
	for i in clients:
		clients[i].kill()
	save_to_chatLog()
	os._exit(0)

def main(pid, num_servers, port):
	global replica, leader, acceptor, listeners, clients, LOG_PATH_REPLICA, LOG_PATH_LEADER, LOG_PATH_ACCEPTOR, crash 
	LOG_PATH_REPLICA = "chatLogs/replica_log{:d}.txt".format(pid)
	LOG_PATH_LEADER = "chatLogs/leader_log{:d}.txt".format(pid)
	LOG_PATH_ACCEPTOR = "chatLogs/acceptor_log{:d}.txt".format(pid)
	make_sure_path_exists("chatLogs")
	crash = Crash() 
	replica = Replica(pid, num_servers, port)
	replica.setDaemon(True)
	acceptor = Acceptor(pid, num_servers, clients)
	print str(pid) + " acceptor initialized"
	acceptor.setDaemon(True)
	acceptor.start()
	# print "before acceptor " + str((acceptor == None))
	with acceptor_cv: 
		acceptor_cv.notifyAll()
		# print str(pid) + " notified acceptor cv"
	# except: 
	# 	print sys.exc_info()[0] 
	leader = Leader(pid, num_servers, clients)
	leader.setDaemon(True)
	with leader_cv: 
		leader_cv.notifyAll()
	# 	print str(pid) + " notified leader cv"
	# except: 
	# 	pass 
	replica.start()
	with replica_cv: 
		replica_cv.notifyAll() 
	# 	print str(pid) + " notified replica cv"
	# except: 
	# 	pass 


if __name__ == "__main__":
	args = sys.argv
	if len(args) != 4:
		print "Need three arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)
