#!/usr/bin/env python
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
from threading import Lock, Thread
from pvalue import Pvalue

listeners = {}
clients = {}

BASEPORT = 22500
LEADER_BASEPORT = 25000
SLEEP = 0.05
BALLOT_NUM = -1
ACCEPTED = []
BALLOT_LOCK = Lock()
ADDR = 'localhost'

def init_acceptor_listeners(aid, num_leaders):
	global listeners
	for i in range(num_leaders):
		listeners[i] = AcceptorListener(aid, i, num_leaders)
		listeners[i].start()

def init_acceptor_senders(aid, num_leaders):
	global clients
	for i in range(num_leaders):
		clients[i] = AcceptorClient(aid, i, num_leaders)
		clients[i].start()


def update_ballot_num(req_type, b, p_val=None):
	global BALLOT_NUM, ACCEPTED
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
	accept_repr = ';'.join(accept_strs)
	if not accept_repr:
		accept_repr = 'none'
	output = '{:d} {}'.format(BALLOT_NUM, accept_repr)
	BALLOT_LOCK.release()
	return output

# only adopt strictly increasing ballot numbers
# add <b,s,p> to accepted if b == ballot_num
# if <b,s,p> is accepted by a and <b,s,p'> is accepted by a' then p = p'
# if a pvalue accepted by majority, 


class AcceptorListener(Thread):
	def __init__(self, aid, lid, num_leaders): 
		Thread.__init__(self)
		self.aid = aid
		self.lid = lid
		self.num_leaders = num_leaders
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = BASEPORT + 2 * aid * num_leaders + lid 
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self):
		global BALLOT_LOCK, BALLOT_NUM
		self.conn, self.addr = self.sock.accept()
		# print "acceptor " + str(self.aid) + " listen to leader " + str(self.lid) + " at port " + str(self.port)
		while True:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				msgs = l.split()
				if msgs[0] == 'p1a':
					num = int(msgs[1])
					print "Acceptor {:d} gets p1a with {:d} from Leader {:d}".format(self.aid, num, self.lid)
					update_ballot_num('p1a', num)
					try: 
						clients[self.lid].send('p1b ' + str(num) + ' ' + state_repr())
					except: 
						"acceptor key error: " + str(self.lid) + " " + str(len(clients)) + " " + str(state_repr())
				elif msgs[0] == 'p2a':
					b_num = msgs[1]
					s_num = msgs[2]
					if len(msgs) < 4: 
						proposal = "None"
					else: 
						proposal = msgs[3]
					# b_num, s_num, proposal = msgs[1:]
					b_num = int(b_num)
					s_num = int(s_num)
					print "Acceptor {:d} gets p2a with {:d}, {:d}, {} from Leader {:d}".format(self.aid, b_num, s_num, proposal, self.lid)
					v = Pvalue(b_num, s_num, proposal)
					update_ballot_num('p2a', b_num, v)
					BALLOT_LOCK.acquire()
					msg = 'p2b ' + str(b_num) + ' ' + str(s_num) + ' ' + str(BALLOT_NUM)
					BALLOT_LOCK.release()
					clients[self.lid].send(msg)
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


class AcceptorClient(Thread):
	def __init__(self, aid, lid, num_leaders): 
		Thread.__init__(self)
		self.aid = aid
		self.lid = lid
		self.target_port = LEADER_BASEPORT + 4 * lid * num_leaders + 2 * num_leaders + aid
		newbase = BASEPORT + 2 * aid * num_leaders
		self.port = newbase + num_leaders + lid
		self.sock = None
		self.connected = False

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
		# 		# print "acceptor " + str(aid) + " send to leader " + str(lid) + " at port " + str(self.target_port) + " from " + str(self.port)
		# 	except Exception as e:
		# 		time.sleep(SLEEP)

	def send(self, msg):
		if not msg.endswith('\n'):
			msg += '\n'
		try:
			self.sock.send(msg)
			print "acceptr " + str(self.aid) + " send " + msg[:-1] + " to leader " + str(self.lid)
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
	  	# print "Acceptor {:d} sends {} to leader {:d}".format(self.aid, msg, self.lid)
