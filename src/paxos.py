#!/usr/bin/env python
import sys;


chatLog = []

class Replica(Thread):
	def __init__(self, pid, num_servers, port):
	global alives, heartbeat_thread
	Thread.__init__(self)
	self.pid = pid
	self.num_servers = num_servers
	self.port = port
	self.buffer = ""
	#Probably need 2n ports instead
	# for i in range(self.num_servers):
	# 	if i != pid:
	# 		listeners[i] = Listener(pid, i)
	# 		listeners[i].start()
	# for i in range(self.num_servers): 
	# 	if (i != pid): 
	# 		clients[i] = Client(pid, i) 
	# 		clients[i].start()
	heartbeat_thread = Heartbeat(pid)
	heartbeat_thread.setDaemon(True)
	heartbeat_thread.start()
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
				cmd = l.split()[0]
				if cmd == "get":
					log = ','.join(chatLog)
					self.master_conn.send('chatLog {}\n'.format(msg))
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



def main(sid, num_servers, port):
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
