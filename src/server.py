#!/usr/bin/env python
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
import os, errno
from threading import Thread, Lock
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
LOG_PATH = None 
crash = None 

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
		self.count = 0 
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
	global chatLog, LOG_PATH, leader, acceptor
	with open(LOG_PATH, 'wt') as file: 
		file.write('{}\n'.format(len(chatLog)))
		for line in chatLog: 
			file.write(line + '\n')
		file.write('{}\n'.format(len(leader.proposals))) 
		for line in leader.proposals: 
			s, p = line 
			file.write(str(s) + " " + str(p) + "\n")
		file.write('{}\n'.format(len(acceptor.accepted)))
		for line in acceptor.accepted: 
			file.write(str(line) + "\n")

def load_from_chatLog(): 
	global chatLog, LOG_PATH, leader, acceptor
	try:
		with open(LOG_PATH, 'rt') as file: 
			chatLog_line = int(file.readline())
			for i in range(chatLog_line): 
				line = file.readline()
				chatLog.append(line)
			proposals_line = int(file.readline())
			for i in range(proposals_line): 
				line = file.readline()
				s, p = line.split(None, 1)
				s = int(s)
				leader.proposals.add((s, p)) 
			accepted_line = int(file.readline())
			for i in range(accepted_line): 
				line = file.readline()
				b, s, c = line.split(None, 2)
				acceptor.accepted_line.append(Pvalue(b, s, c))
	except: 
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
		self.proposals = set() 
		self.decisions = set()
		for i in range(num_servers):
			if i != pid:
				listeners[i] = ServerListener(pid, i, num_servers)
				# listeners[i].setDaemon(True)
				listeners[i].start()
		for i in range(num_servers): 
			if i != pid: 
				clients[i] = ServerClient(pid, i, num_servers) 
				# clients[i].setDaemon(True)
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
		load_from_chatLog()
		while self.connected:
			if '\n' in self.buffer:
				if not self.leader_initialized: 
					leader.start()
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				try: 
					(cmd, arguments) = l.split(" ", 1)
				except: 
					print l 
				print "Replica {:d} receives msgs from master {}".format(self.pid, l)
				if cmd == "get":
					self.master_conn.send('chatLog {}\n'.format(state.toString()))
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
					for i in lst: 
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
					self.master_conn.close()
					self.master_conn = None 
					self.master_conn, self.master_addr = self.socket.accept()

	def decide(self, arguments): 
		s, p = arguments.split(" ", 1)
		self.decisions.add((s, p))
		print "leader: " + str(self.pid) + " self.decisions: " + str(self.decisions)		
		while True: 
			pair = None 
			remove_t = None 
			for t in self.decisions: 
				s1, p1 = t 
				s1 = int(s1)
				if s1 == self.slot_number: 
					pair = (s1, p1) 
					remove_t = t 
					break
			# print "pair: " + str(pair)
			if pair == None: 
				break
			s1, p1 = pair 
			for t in self.proposals: 
				s2, p2 = t 
				if s2 == self.slot_number and p1 != p2: 
					self.propose(p2)
					break 
			self.decisions.remove(remove_t)
			self.perform(s1, p1)

	def propose(self, p):
		global leader
		found = False 
		for t in self.decisions: 
			(s, p_prime) = t 
			if p == p_prime:
				found = True 
				break
		if not found: 
			total_set = self.decisions.union(self.proposals)
			all_slots_taken = [s for (s, p) in total_set]
			if len(all_slots_taken) == 0: 
				upper_bound = 2
			else:
				upper_bound = max(all_slots_taken) + 2 
			s_prime = 1
			for i in range (1, upper_bound): 
				if i not in all_slots_taken: 
					s_prime = i
					break 
			proposal = s_prime, p
			self.proposals.add(proposal)
			leader.add_proposal(proposal)


	def perform(self,s, p): 
		# print "in perform"
		cid, op = p.split(" ", 1)
		found = False 
		for i in self.decisions: 
			s_prime, p_prime = i 
			if s_prime < self.slot_number and p == p_prime: 
				found = True 
				break 
		if found: 
			self.slot_number += 1 
		else: 
			global state 
			result = state.op(s, p) 
			self.slot_number += 1 
			print "Replica {:d} sends ACK back to master: {}".format(self.pid, str(result))
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
		global leader, acceptor, replica
		self.conn, self.addr = self.sock.accept()
		# print "Server " + str(self.pid) + " listen to Server " + str(self.target_pid) + " at port " + str(self.port)
		while True:
			if '\n' in self.buffer:
				l, rest = self.buffer.split('\n', 1)
				self.buffer = rest
				cmd, info = l.split(None, 1)
				# leader receives
				if cmd == 'p1b':
					leader.process_p1b(self.target_pid, info)
				elif cmd == 'p2b':
					leader.process_p2b(self.target_pid, info)
				# accpetor recieves
				elif cmd == 'p1a':
					acceptor.process_p1a(self.target_pid, info)
				elif cmd == 'p2a':
					acceptor.process_p2a(self.target_pid, info)
				elif cmd == 'decision':
					print "Server " + str(self.pid) + " received decision from leader " + str(self.target_pid)
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
		while True: 
			try: 
				self.sock.send("heartbeat " + str(self.pid) + "\n")
			except:
				try:
					self.sock = None
					s = socket(AF_INET, SOCK_STREAM)
					s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
					# s.bind((ADDR, self.port))
					s.connect((ADDR, self.target_port))
					# print "serverclient " + str(self.pid) + " connected to " + str(self.target_pid)
					self.sock = s 
					self.sock.send("heartbeat " + str(self.pid) + "\n")
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
			print "crashing p2a"
			self.forward_msg(msg)
			exit()
		else: 
			self.forward_msg(msg)

	def forward_msg(self, msg): 
		if not msg.endswith("\n"): 
			msg = msg + "\n"
		try: 
			self.sock.send(msg)
		except: 
			# try: 
			self.sock = None 
			s = socket(AF_INET, SOCK_STREAM)
			s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
			# s.bind((ADDR, self.port))
			s.connect((ADDR, self.target_port))
			self.sock = s 
			self.sock.send(msg)
			# except:
			# 	print "***************************** " 
			# 	time.sleep(SLEEP)


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
	global replica, leader, acceptor, listeners, clients, LOG_PATH, crash 
	LOG_PATH = "chatLogs/log{:d}.txt".format(pid)
	make_sure_path_exists("chatLogs")
	crash = Crash() 
	replica = Replica(pid, num_servers, port)
	replica.setDaemon(True)
	replica.start() 
	acceptor = Acceptor(pid, num_servers, clients)
	acceptor.setDaemon(True)
	acceptor.start()
	leader = Leader(pid, num_servers, clients)
	leader.setDaemon(True)

if __name__ == "__main__":
	args = sys.argv
	if len(args) != 4:
		print "Need three arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)
