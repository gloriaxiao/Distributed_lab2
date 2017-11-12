#!/usr/bin/env python
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error


listeners = []
clients = []
BASEPORT = 20000

class pvalue:
	def __init__(self, ballot_num, slot_num, c):
		self.ballot = ballot_num
		self.slot = slot_num
		self.command = c

# only adopt strictly increasing ballot numbers
# add <b,s,p> to accepted if b == ballot_num
# if <b,s,p> is accepted by a and <b,s,p'> is accepted by a' then p = p'
# if a pvalue accepted by majority, 
class Accepter:

	def __init__(self, pid, num_leaders):
		self.aid = pid
		self.ballot_num = 0
		self.accepted = []
		self.num_leaders = num_leaders
		for i in range(self.num_leaders):
			listener[i] = AccepterListener(pid, i, num_leaders)
			listener[i].start()
		for i in range(self.num_leaders):
			clients[i] = AccepterClient(pid, i, num_leaders)
			clients[i].start()


class AccepterListener:
	def __init__(self, aid, lid, num_leaders): 
		Thread.__init__(self)
		self.aid = aid
		self.lid = lid
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = BASEPORT + aid * num_leaders + lid 
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self):
		self.conn, self.addr = self.sock.accept() e

