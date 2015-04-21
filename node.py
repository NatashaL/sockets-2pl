import threading
import socket
from utils import *
from time import sleep
import random
import copy
import sys
import os
import re

ACTIVE_NODES = []			#Keeps track of active nodes. Once a node crashes, it cannot be activated again
TIMEOUT = 10**5				#A timeout of 10^5 cycles is set for receiving expected responses (lock_response and ack) from other nodes. After this timeout has passed, the nodes that haven't replied are considered dead.

THIS_NODE = ''				#A name for this node. Predefined as 'NODE<IDX>' where <IDX> is given as input argument
THIS_IP = ''				#The IP address of this node
THIS_PORT = -1				#The port on which it listens
THIS_DB = ''				#The name od the database. Predefined as 'NODE<IDX>/' where <IDX> is given as input argument

MAX_FRAME_SIZE = 100000			#Maximum size of a correspondence message between nodes
sock = None				#The socket object for this is made global and initially None. Its value is set in __main__()

stop_flag = False			#There is no thread.destroy() method. We use a flag instead to signal the thread to exit safely
	
PHASE1_LOCKS = {}			#A map of all the variables in the database. Key = name of variable, Value = True/False (Locked/Not locked)

ACK_BUFFER = []				#A list of ACK responses that are awaited to arrive from the other nodes in the network. When an ACK is received, it is removed from the list
RESPONSE_BUFFER = []			#A list(buffer) in which the LOCK_RESPONSE messages are buffered and waiting to be processed.

mutex_db_ack = threading.Lock()		#A lock for accessing the ACK_BUFFER list
mutex_lock_response = threading.Lock()	#A lock for accessing the RESPONSE_BUFFER list




#
# ->query is a string representing the operations in the current transaction
# 	- query inherently contains the variables (tokens) that need to be locked
#
# This function takes the query from the user input, identifies the variable tokens
# and locks all of them in the order in which they appear.
# It sends a lock request to all other active nodes and waits for a response.
#
def lock(query):				
	tokens = get_tokens(query)	#extract the variables contained in the query expression
	tokens = list(set(tokens))	#remove duplicates

	#As long as any other node has a lock on any of the tokens, repeat the cycle
	while 1:
		for token in tokens:
			PHASE1_LOCKS[token] = True

		#send lock request to peers
		request = copy.deepcopy(frame)
		request['sender'] = THIS_NODE
		request['type'] = LOCK_REQUEST
		request['lock_vars'] = tokens
		
		print 'Sent a lock request to:'
		send_to_active_peers(request)

		result = recv_response(request)
		print 'Cumulative response to lock request is ', '\'YES\'' if result else '\'NO\''
	
		if result == False:
			unlock(query)
			sleep(random.random()*15)
			continue

		return
	
#
# This function takes as argument the lock request that has been sent, 
# and waits for a response that corresponds to that request.
# The response received is checked (whether it is yes/no and which sender sends it)
# and depending on the response it returns True/False - which means whether the locks have been allowed from all other active nodes.
# If some nodes didn't respond in a certain time, they are considered as inactive.
#
def recv_response(request):

	responses = []					#A list of booleans (responses from peers)
	global ACTIVE_NODES
	
	timeout = 0					#Set cycle counter for timeout
	active = []					#A list of nodes from which a reponse has been received
	while len(responses) < len(ACTIVE_NODES) and timeout < TIMEOUT:
		timeout+=1
		mutex_lock_response.acquire(True)
		global RESPONSE_BUFFER

		if len(RESPONSE_BUFFER) == 0:		#If response buffer is empty, try again in next cycle
			mutex_lock_response.release()
			continue

		RESPONSE = RESPONSE_BUFFER.pop()	#Otherwise pop the last response message in the buffer and process it

		print 'Received response: ', pretify_msg(RESPONSE)

		if RESPONSE['response'] == None:	#If for some reason the response is None, ignore message and try again in next cycle
			mutex_lock_response.release()
			continue

		if set(request['lock_vars']) != set(request['lock_vars']):		#If the variables that are supposed to be locked are not the same in the request and in the response, maybe this response is for some other request. Put it back in the beginning of the list and continue proccessing the ones at the end of the list.
			RESPONSE_BUFFER = [RESPONSE] + RESPONSE_BUFFER
			mutex_lock_response.release()		
			raise Exception('unknown sender')			

		if RESPONSE['sender'] not in [x for node in ACTIVE_NODES for x in node]:	#If the sender of the response is not active, then this is an old message. Ignore it.
			mutex_lock_response.release()		
			raise Exception('unknown sender')

		if RESPONSE['response'] == 'YES':	#If the response is yes, 
			for node in ACTIVE_NODES:
				if RESPONSE['sender'] == node[0]:
					active += [node[0]]	#add the sender in the list of active nodes and ..
					responses += [True]	#add its reponse in the list of responses
		elif RESPONSE['response'] == 'NO':	
			for node in ACTIVE_NODES:
				if RESPONSE['sender'] == node[0]:
					active += [node[0]]	#add the sender in the list of active nodes and ..
					responses += [False]	#add its reponse in the list of responses
		else:					#If the response is something else, throw an exception
			mutex_lock_response.release()
			raise Exception('unknown response code')
			
		RESPONSE = copy.deepcopy(frame)
		mutex_lock_response.release()

	if timeout == TIMEOUT:				#If the cycle has ended because of a timeout,
		peers_left = []

		for node in active:
			for peer in ACTIVE_NODES:
				if peer[0] == node:
					peers_left += [peer]		#collect all active nodes and ..
					break
		ACTIVE_NODES = peers_left				#update the list of active nodes

		return len(ACTIVE_NODES) == 0 or all(responses)		#if all nodes are dead or all responses are True, return True (the locks can be acquired)

	return all(responses)						#If the cycle ended because all the expected responses arrived, return True if all are True, otherwise False (the locks cannot be acquired)
	
#
# Returns a human-readable form of the message.
#
def pretify_msg(msg):
	string = ''
	string += 'Message type:   ' + str(msg['type']) + '\n'
	string += 'Sender:         ' + str(msg['sender']) + '\n'
	string += 'Query:          ' + str(msg['query']) + '\n'
	string += 'Lock variables: ' + str(msg['lock_vars']).replace('\'','')[1:-1] + '\n'
	string += 'Ack:            ' + str(msg['ack']) + '\n'
	string += 'Response:       ' + str(msg['response']) + '\n'
	return string

#
# Returns a formatted string of the node's info
#
def pretify_node(node):
	return 'Name: %s\t IP: %s\t Port: %d' %(node[0],node[1],node[2])

#
# Unlocks all the variable tokens from 'query' in reverse order.
#
def unlock(query):
	tokens = get_tokens(query)
	tokens = list(set(tokens)) 		#remove duplicates
	for token in tokens[::-1]:		#release all locks in reverse order of acquiring
		PHASE1_LOCKS[token] = False

#
# Constructs an update message to be sent to all the active nodes with the details from the current transaction,
# and calls the send function.
#
def update_peers(query):			#->query is the expression that updated the database
	msg = copy.deepcopy(frame)
	msg['sender'] = THIS_NODE
	msg['type'] = DB_UPDATE_QUE
	msg['query'] = query			#The same query should be sent to all other databases to be updated

	add_to_ack_buffer(query)		#Fill up the ACK_BUFFER with the ACKs that are expected from the other nodes
	send_to_active_peers(msg)		#Send the UPDATE message

	recv_ACK(query)				#Process ACKs

#
# Sends the message 'msg' to all currently active nodes.
#
def send_to_active_peers(msg):			#Send the message 'msg' to all active nodes
	for peer in ACTIVE_NODES:
		if peer[0] == THIS_NODE: continue
		print pretify_node(peer)
		sock.sendto(str(msg)+'\n',(peer[1],peer[2]))

#
# Each update query that awaits ACKs from the other nodes lives in an ACK_BUFFER until all ACKs are received.
# This function adds the argument query to the ACK_BUFFER.
#
def add_to_ack_buffer(query):			#Fill up the ACK_BUFFER
	mutex_db_ack.acquire(True)

	global ACK_BUFFER
	ACK_BUFFER = [query + node[0] for node in ACTIVE_NODES]

	mutex_db_ack.release()

#
# When an update query has been sent, this function is called to wait for all the ACKs.
# If all the ACKs are received, the function returns regularly, and if not, 
# the nodes that haven't responded are removed from the ACTIVE_NODES list
#
def recv_ACK(query):
	global ACTIVE_NODES			#The global variable ACTIVE_NODES will be changed in this function

	timeout = 0				#Set cycle counter for timeout
	while len(ACK_BUFFER) > 0 and timeout < TIMEOUT:
		timeout += 1
		continue

	if len(ACK_BUFFER) == 0:		#If ACK_BUFFER is empty, then all the ACKs have been received. Return normally.
		return

	for ack in ACK_BUFFER:			#Otherwise, find only the active nodes and ..
		node = ack[-4:]
		peers_left = copy.deepcopy(ACTIVE_NODES)
		for peer in ACTIVE_NODES:	
			if peer[0] == node:
				peers_left.remove(peer)
		ACTIVE_NODES = peers_left	#update the ACTIVE_NODES list

#
# This function is constantly running in a separate thread trying to read from the socket's input buffer.
# When it successfully reads a message, it parses it, determines the type 
# and calls the function that is responsible for handling that type of messages.
#
def read_socket():
	while 1:
		if stop_flag:			#Because there is no thread.destroy() method. We use a flag instead to signal the thread to exit safely
			return

		buff = None
		try:
			buff = sock.recv(MAX_FRAME_SIZE)
		except:
			continue

		if buff == None: continue

		msg = eval(buff)		#The message received can be one of four possible types:
		if msg['type'] == LOCK_REQUEST:
			process_lock_request(msg['sender'],msg['lock_vars'])
		if msg['type'] == LOCK_RESPONSE:
			process_lock_response(msg)
		if msg['type'] == DB_UPDATE_QUE:
			process_db_update_query(msg['sender'],msg['query'])
		if msg['type'] == DB_UPDATE_ACK:
			process_db_update_ack(msg['sender'],msg['ack'])
		
#
# When a lock request has been received, the request is parsed, 
# and all the variables that are requested to be locked are checked 
# and the result (YES/NO) is added to the response.
# The response is the sent only to the sender of the request.
#
def process_lock_request(sender,lock_vars):
	response = 'YES'	
	for var in lock_vars:
		if var in PHASE1_LOCKS and PHASE1_LOCKS[var] == True:	#If any of the variables requested are currently locally locked, set response to 'NO'
			response = 'NO'
			break

	#construct the response message
	msg = copy.deepcopy(frame)
	msg['sender'] = THIS_NODE
	msg['type'] = LOCK_RESPONSE
	msg['response'] = response			#can change this value to 'NO' in order to test 2 PHASE LOCKING
	msg['lock_vars'] = lock_vars

	for node in ACTIVE_NODES:			
		if sender == node[0]:			#Send the response message to the node that requested the locks
			print 'Lock response sent to ', node[0]
			print pretify_msg(msg)
			sock.sendto(str(msg)+'\n',(node[1],node[2]))

#
# When a response is received for a lock request that this node has sent to all the other nodes,
# this function is called to add the response to the RESPONSE_BUFFER. 
# The RESPONSE_BUFFER is later processed in the recv_response(..) function
#
def process_lock_response(response):

	mutex_lock_response.acquire(True)

	global RESPONSE_BUFFER				#Add the received response message to the RESPONSE_BUFFER
	RESPONSE_BUFFER += [response]

	mutex_lock_response.release()

#
# When a node has executed a transaction it sends an update query to all other nodes.
# This function processes that update query message and updates all the changed variables in its local database.
# It then sends an ACK message that the update has been finished.
#
def process_db_update_query(sender,query):
	#query looks like: var1=value1; var2=value2; ....
	expressions = query.split(';')
	for exp in expressions:
		m = re.match(r'\s*([a-zA-Z]+\w*)\s*=(.*)$',exp)
		left,right = m.group(1).strip(),m.group(2).strip()
		set_var(THIS_DB,left,right)

	#construct ack
	msg = copy.deepcopy(frame)
	msg['sender'] = THIS_NODE
	msg['type'] = DB_UPDATE_ACK
	msg['ack'] = query

	for node in ACTIVE_NODES:
		if sender == node[0]:			#Send the ACK to the node that sent the update
			sock.sendto(str(msg)+'\n',(node[1],node[2]))

#
# This function processes the ACK messages, 
# by removing their corresponding entry in the ACK_BUFFER.
# (ACK_BUFFER holds info about the ACKs that are expected.)
#
def process_db_update_ack(sender,ack):

	mutex_db_ack.acquire(True)
	if ack + sender in ACK_BUFFER:			#Process the received ACK message i.e. remove it from the ACK_BUFFER.
		ACK_BUFFER.remove(ack + sender)
	mutex_db_ack.release()


#
# Main method.
# The program arguments (hostfile, index) are parsed.
# The socket is created.
# The database is created (if it doesn't already exist).
# The locks are initialized (all False).
# The thread that reads from the sockets input buffer is started.
# 
# User input is then awaited.
# Each user string is sent to lock(..) that tries to parse the variable tokens and lock the variables.
# After locking, the string is checked to see if it is a valid mathematical expression.
# 	if it is not, the locks are released.
# Otherwise, the expression is executed and an update_query string is constructed 
#   in order to be sent to the other nodes.
# 
# All other nodes all updated, and after update the locks are released.
#

def __main__():
	
	if len(sys.argv) != 3:
		print 'Usage: python node1.py <hostfile> <index>'
		print '- arg1 is the file containing host adresses and ports'
		print '- arg2 is the index of the node in the hostfile'
		sys.exit(1)
	elif not os.path.isfile(sys.argv[1]):
		print sys.argv[1] , 'is not a valid filename'
		sys.exit(1)
	elif type(int(sys.argv[2])) != int:
		print sys.argv[2], 'is not an integer'
		print 'arg2 should be an integer'
		sys.exit(1)
	print 'checking hostfile..'
	hostfile = open(sys.argv[1],'r')
	lines = hostfile.readlines()
	hostfile.close()
	this_index = int(sys.argv[2])
	global ACTIVE_NODES, THIS_NODE,THIS_IP, THIS_PORT, THIS_DB, sock

	THIS_NODE = 'NODE' + sys.argv[2]
	try:
		for idx,line in enumerate(lines):
			line = line.strip()
			address,port = re.split(r'\s+',line)
			node = 'NODE' + str(idx)
			port = int(port)
			if idx == this_index:
				THIS_IP = address
				THIS_PORT = port
				THIS_DB = THIS_NODE + '/'
			ACTIVE_NODES += [[node,address,port]]
	except Exception as e:
		print 'An error occured while reading the hostfile'
		print e
		sys.exit(1)

	print 'creating and binding the socket..'
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

	if sock == None:
		print 'Could not create a socket'
		sys.exit(1)

	#make the socket non-blocking
	sock.setblocking(0)

	for node in ACTIVE_NODES:
		node_info = socket.gethostbyname(node[1])
		if node_info == None:
			print 'initSocketAddress - Unknown host %s' % node[1]
			sys.exit(1)
		#print node_info

	#bind doesn't return a value. It throws an error if something goes wrong
	try:	
		sock.bind((THIS_IP,THIS_PORT))
	except Exception as e:
		print 'Binding of this node (%s) failed' % THIS_IP
		print e
		sys.exit(1)

	create_local_database(THIS_DB)
	PHASE1_LOCKS = list_database_contents(THIS_DB)
	#start the thread that reads from the socket
	t = threading.Thread(target=read_socket)
	t.start()

	#send data to the server
	print "\nType an expression to update the database."
	print "Type 'quit' to end this program."

	while 1:
		print '>',
		expression = raw_input()
		if expression == '': continue
		if expression.lower() == 'quit':
			sock.close()
			global stop_flag
			stop_flag = True
			sys.exit(1)
		print 'Acquiring locks...'
		lock(expression)
		print 'Locked..'

		print 'Checking if expression is valid..'
		valid = check_if_valid(THIS_DB,expression)
		print 'Expression is %svalid.' % ('' if valid else 'not ')
		if not valid: 
			unlock(expression)		
			continue

		print 'Executing query..'
		update_query = execute_local_query(THIS_DB,expression)
		print 'Query executed'

		print 'Updating peers..'
		update_peers(update_query)
		print 'Peers updated'

		unlock(expression)
		print 'Locks released'


__main__()	#The main method is called here.

if sock != None:
	sock.close()
