#!/usr/bin/env python
import sys

TIMEOUT = 0.2
SLEEP = 0.05
ADDR = 'localhost'

alives = {} 
self_pid = -1
max_num_servers = -1

leader_listeners = {}
leader_senders = {} 
accepter_listeners = {} 
accepter_senders = {} 
state = {} 

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

class Heartbeat(Thread): 
	def __init__(self, index): 
		Thread.__init__(self)
		self.index = index

	def run(self): 
		global alives
		while True: 
			new_alives = {} 
			now = time.time()
			for key in alives: 
				if now - alives[key] <= 0.2: 
					new_alives[key] = alives[key]
			alives = new_alives
			time.sleep(0.2)

class Replica(Thread):
	def __init__(self, pid, num_servers, port):
		global alives, heartbeat_thread
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.port = port
		self.buffer = ""
		self.state = State() 
		for i in range(self.num_servers):
			if i != pid:
				leader_listeners[i] = LeaderListener(pid, i)
				leader_listeners[i].start()
		for i in range(self.num_servers): 
			if (i != pid): 
				leader_senders[i] = LeaderSender(pid, i) 
				leader_senders[i].start()
		for i in range(self.num_servers):
			if i != pid:
				acceptor_listener[i] = AcceptorListener(pid, i)
				acceptor_listener[i].start()
		for i in range(self.num_servers): 
			if (i != pid): 
				acceptor_senders[i] = AcceptorSender(pid, i) 
				acceptor_senders[i].start()
		heartbeat_thread = Heartbeat(pid)
		heartbeat_thread.setDaemon(True)
		heartbeat_thread.start()
		self.state = initial_state
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
		global chatLog
		while self.connected:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				(cmd, arguments) = l.split(" ", 1)
				if cmd == "get":
					log = ','.join(chatLog)
					self.master_conn.send('chatLog {}\n'.format(msg))
				elif cmd == "msg": 
					self.propose(arguments) 
				elif cmd == "decision": 
					# from leader 
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
			for leader in leader_senders: 
				leader.send("propose " + str(s_prime) + " " + p)

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
			self.master_conn.send("ack " + str(cid) + " " + str(result))

def main(pid, num_servers, port):
	pass 


if __name__ == "__main__":
	args = sys.argv
	if len(args) != 4:
		print "Need three arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)
