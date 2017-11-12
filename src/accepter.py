#!/usr/bin/env python
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
from threading import Lock

listeners = {}
clients = {}

BASEPORT = 22500
LEADER_BASEPORT = 25000
SLEEP = 0.05
BALLOT_NUM = -1
ACCEPTED = []
BALLOT_LOCK = Lock()
ADDR = 'localhost'


def init_accepter(aid, num_leaders):
	for i in range(self.num_leaders):
		listener[i] = AccepterListener(pid, i, num_leaders)
		listener[i].start()
	for i in range(self.num_leaders):
		clients[i] = AccepterClient(pid, i, num_leaders)
		clients[i].start()


def update_ballot_num(req_type, lid, b, p_val=None):
	global BALLOT_NUM
	BALLOT_LOCK.acquire()
	if req_type == 'p1a':
		if b > BALLOT_NUM:	
			BALLOT_NUM = b
	elif req_type == 'p2a':
		if p_val and b >= BALLOT_NUM:
			ACCEPTED.append(p_val)
	BALLOT_LOCK.release()


def state_repr():
	global BALLOT_NUM, ACCEPTED
	BALLOT_LOCK.acquire()
	accept_strs = [str(v) for v in ACCEPTED]
	output = '{:d} {}'.format(BALLOT_NUM, ' '.join(accept_strs))
	BALLOT_LOCK.release()
	return output


class Pvalue:
	def __init__(self, ballot_num, slot_num, c):
		self.ballot = ballot_num
		self.slot = slot_num
		self.command = c

	def __str__(self):
		return "{:d} {:d} {}".format(self.ballot, self.slot, self.command)

# only adopt strictly increasing ballot numbers
# add <b,s,p> to accepted if b == ballot_num
# if <b,s,p> is accepted by a and <b,s,p'> is accepted by a' then p = p'
# if a pvalue accepted by majority, 


class AccepterListener:
	def __init__(self, aid, lid, num_leaders): 
		Thread.__init__(self)
		self.aid = aid
		self.lid = lid
		self.num_leaders = num_leaders
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = BASEPORT + aid * num_leaders + lid 
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self):
		global BALLOT_LOCK, BALLOT_NUM
		self.conn, self.addr = self.sock.accept()
		while True:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				msgs = l.split()
				if msgs[0] == 'p1a':
					num = int(msgs[1])*num_leaders + lid
					update_ballot_num('p1a', num)
					clients[self.lid].send('p1b ' + state_repr())
				elif msgs[0] == 'p2a':
					b_num, s_num, proposal = msgs[1:-1]
					b_num = int(b_num)*num_leaders + lid
					v = Pvalue(b_num, int(s_num), proposal)
					update_ballot_num('p2a', b_num, v)
					BALLOT_LOCK.acquire()
					clients[self.lid].send('p2b ' + str(BALLOT_NUM))
					BALLOT_LOCK.release()
			else:
				try:
					data = self.conn.recv(1024)
					if data == "":
						raise ValueError
					self.buffer += data 
				except Exception as e:
					print str(self.aid) + " lose connection to leader " + str(self.lid)
					self.conn.close()
					self.conn = None 
					self.conn, self.addr = self.sock.accept()


class AccepterClient:
	def __init__(self, aid, lid, num_leaders): 
		Thread.__init__(self)
		self.aid = aid
		self.lid = lid
		self.target_port = LEADER_BASEPORT + lid*num_leaders + aid
		newbase = BASEPORT + 2*aid*num_leaders
		self.port = newbase + num_leaders + lid
		self.sock = None

	def run(self):
		pass

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

