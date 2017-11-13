# def len_list(lst, lock): 
# 	lock.acquire()
# 	length = len(lst)
# 	lock.release()
# 	return length

# p1b_msgs = []
# p1b_lock = Lock() 
# p2b_msgs = [] 
# p2b_lock = Lock() 

if len_list(p2b_msgs, p2b_lock) != 0: 
			alpha, b = pop_head_list(p2b_msgs, p2b_lock)
			if b_prime != b: 
				waitfor.discard(alpha)
				if len(waitfor) < len(max_num_servers) < 2: 
					for i in sender_to_replicas: 
						sender_to_replicas[i].send("decision " + str(s) + " " + str(p))
						return 
			else: 
				send_preempted_to_leader(b_prime)
				return
# if len_list(p1b_msgs, p1b_lock) != 0: 
# 	alpha, msg = pop_head_list(p1b_msgs, p1b_lock)
# 	b_prime, r = msg.split(" ", 1)
# 	if b == b_prime: 
# 		pvalues = pvalues.union(r)
# 		set.discard(alpha)
# 		if len(waitfor) < max_num_servers / 2: 
# 			send_adopted_to_leader(b, pvalues)
# 			return 
# 	else: 
# 		send_preempted_to_leader(b_prime)
# 		return

def append_to_list(lst, lock, msg): 
	lock.acquire() 
	lst.append(msg)
	lock.release()

def pop_head_list(lst, lock): 
	lock.acquire()
	head = lst[0]
	lst = lst[1:]
	lock.release()
	return head 


def send_adopted_to_leader(b, pvalues): 
	append_to_list(adopted_msgs, adopted_lock, (b, pvalues)) 

def send_preempted_to_leader(b): 
	append_to_list(preempted_msgs, preempted_lock, b) 
