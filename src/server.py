#!/usr/bin/env python
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
import os
from threading import Thread, Lock

master_thread = None
SLEEP = 0.05
ADDR = 'localhost'
BASEPORT = 20000

Listeners = {}
Clients = {}


class MasterListener(Thread):
	def __init__(self, pid, num_servers, port):
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.port = port
		self.buffer = ""
		for i in range(self.num_servers):
			if i != pid:
				listeners[i] = ServerListener(pid, i)
				listeners[i].start()
		for i in range(self.num_servers): 
			if (i != pid): 
				clients[i] = ServerClient(pid, i) 
				clients[i].start()
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True

	def run(self):
		while self.connected:
			if '\n' in self.buffer:
				l, rest = self.buffer.split('\n', 1)
				print "Server {:d} receives {} from master".format(self.pid, l)
				self.buffer = rest
				cmd = l.split()[0]
				






def make_sure_path_exists(path):
	try:
		os.makedirs(path)
	except OSError as e:
		if e.errno != errno.EEXIST:
			print("Error: Path couldn't be recognized!")
			print(e)


def main(pid, num_servers, port):
	# print "starting main"
	global master_thread
	LOG_PATH = "chatLogs/log{:d}.txt".format(pid)
	make_sure_path_exists("chatLogs")
	master_thread = MasterListener(pid, num_servers, port)
	master_thread.start()


if __name__ == "__main__":
	args = sys.argv
	if len(args) != 4:
		print "Need three arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)
