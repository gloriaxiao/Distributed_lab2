#!/usr/bin/env python
import sys
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
import time
import os
from threading import Thread, Lock
from replica_1 import Replica
from leader_1 import Leader
from acceptor_1 import Acceptor

master_thread = None
SLEEP = 0.05
ADDR = 'localhost'
BASEPORT = 20000
chatLog = []
replica = None
leader = None
acceptor = None

class Server:
	def __init__(self, )
		self.listeners = {}
		self.clients = {}
		for i in range(self.num_servers):
			if i != pid:
				self.listeners[i] = ServerListener(pid, i)
				self.listeners[i].start()
		for i in range(self.num_servers): 
			if (i != pid): 
				self.clients[i] = ServerClient(pid, i) 
				self.clients[i].start()
		self.master_thread = MasterListener(pid, num_servers, port)
		self.master_thread.start()

class MasterListener(Thread):
	def __init__(self, pid, num_servers, port):
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.port = port
		self.buffer = ""
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
				(cmd, arguments) = l.split(" ", 1)
				if cmd == "get":
					self.master_conn.send('chatLog {}\n'.format("\n".join(chatLog)))
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

				

class ServerListener(Thread):
	def __init__(self, pid, target_pid, num_servers): 
		Thread.__init__(self)
		self.pid = pid
		self.target_pid = target_pid
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = BASEPORT + 2 * pid * num_servers + target_pid
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self):
		self.conn, self.addr = self.sock.accept()
		print "Server " + str(self.pid) + " listen to Server " + str(self.target_pid) + " at port " + str(self.port)



class ServerClient(Thread):
	def __init__(self, pid, target_pid):
	  	Thread.__init__(self)
	  	self.pid = pid
	  	self.target_pid = target_pid 
	  	self.target_port = BASEPORT + 2*target_pid*num_servers + pid
	  	self.port = BASEPORT + 2 * pid * num_servers + num_servers + target_pid
	  	self.sock = None
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
				print "State " + str(self.pid) + " sender to State " + str(self.target_pid) + " at port " + str(self.target_port) + " from " + str(self.port)
			except Exception as e:
				time.sleep(SLEEP)

	def send(self, msg): 
	  	if not msg.endswith("\n"): 
	  		msg = msg + "\n"
	  	try:
	  		self.sock.send(msg)
	  	except Exception as e:
	  		if self.sock:
	  			self.sock.close()
	  			self.sock = None
	  		try:
	  			new_socket = socket(AF_INET, SOCK_STREAM)
				new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				new_socket.connect((ADDR, self.target_port))
				self.sock = new_socket
				self.sock.send(msg)
			except:
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


def main(pid, num_servers, port):
	global replica, leader, acceptor
	LOG_PATH = "chatLogs/log{:d}.txt".format(pid)
	make_sure_path_exists("chatLogs")
	server = Server(pid, num_servers, port)
	replica = Replica(server)
	leader = Leader(server)
	acceptor = Acceptor(server)


if __name__ == "__main__":
	args = sys.argv
	if len(args) != 4:
		print "Need three arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)
