#!/usr/bin/env python
from threading import Lock, Thread

class Pvalue:
	def __init__(self, ballot_num, slot_num, c):
		self.ballot = ballot_num
		self.slot = slot_num
		self.command = c

	def __str__(self):
		return "{:d} {:d} {}".format(self.ballot, self.slot, self.command)


class Acceptor(Thread):
	def __init__(self, pid, num_servers, clients):
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.clients = clients
		self.ballot_num = -1
		self.accepted = []
		self.b_lock = Lock()

	def run(self):
		pass

	def update_ballot_num(self, req_type, b, p_val=None):
		self.b_lock.acquire()
		if req_type == 'p1a':
			if b > self.ballot_num:	
				self.ballot_num = b
		elif req_type == 'p2a':
			if p_val and b >= self.ballot_num:
				self.accepted.append(p_val)
		self.b_lock.release()

	def process_p1a(self, target_pid, info):
		msgs = info.split()
		num = int(msgs[0])
		# print "Acceptor {:d} gets p1a with {:d} from Leader {:d}".format(self.pid, num, target_pid)
		self.update_ballot_num('p1a', num)
		try: 
			self.clients[target_pid].send('p1b ' + str(num) + ' ' + self.state_str())
		except:
			"acceptor key error: " + str(self.pid) + " " + str(len(self.clients)) + " " + self.state_str()	

	def process_p2a(self, target_pid, info):
		msgs = info.split()
		b_num = int(msgs[0])
		s_num = int(msgs[1])
		if len(msgs) < 3: 
			proposal = "None"
		else:
			proposal = msgs[2]
			# b_num, s_num, proposal = msgs[1:]
		# print "Acceptor {:d} gets p2a with {:d}, {:d}, {} from Leader {:d}".format(self.pid, b_num, s_num, proposal, target_pid)
		v = Pvalue(b_num, s_num, proposal)
		self.update_ballot_num('p2a', b_num, v)
		self.b_lock.acquire()
		msg = 'p2b ' + msgs[0] + ' ' + msgs[1] + ' ' + str(self.ballot_num)
		self.b_lock.release()
		self.clients[target_pid].send(msg)

	def state_str(self):
		self.b_lock.acquire()
		accept_strs = [str(v) for v in self.accepted]
		accept_repr = ';'.join(accept_strs)
		if not accept_repr:
			accept_repr = 'none'
		output = '{:d} {}'.format(self.ballot_num, accept_repr)
		self.b_lock.release()
		return output

