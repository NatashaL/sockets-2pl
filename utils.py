import sys
import os
import re


#
#Possible message types
#
LOCK_REQUEST = 'lock_req'
LOCK_RESPONSE = 'lock_resp'
DB_UPDATE_QUE = 'update'
DB_UPDATE_ACK = 'ack'

#
#Message structure
#
frame = { 'type':None,		#one of the four possible message types
	 'sender': None,	 
	 'lock_vars':None,	#if type is LOCK_REQ or LOCK_RESPONSE, this is a list of variable names to lock
	 'response':None,	#if type is LOCK_RESP, this is 'YES'/'NO'
	 'query':None,		#if type is UPDATE, this is an expression formated as: 'var1 = value1; var2 = value2; ....'
	 'ack':None		#if type is ACK, then this argument is the expression that has been updated
	}

#
#From the give query (transaction), extracts all potential variables
#ex. get_tokens('a = 4; b = a * a; c = d')    -> [a,b,c,d]
#
def get_tokens(query):
	tokens = re.findall(r'([a-zA-Z0-9]+)',query)
	invalid_tokens = []
	for token in tokens:
		if len(re.findall(r'^[a-zA-Z]',token)) == 0:
			invalid_tokens += [token]

	for token in invalid_tokens:
		tokens.remove(token)
	
	return tokens

#
#Takes a single expression from the transaction, and splits it into a left and right expression according to the '=' sign
#
def get_left_right_expression(query):
	m = re.match(r'\s*([a-zA-Z]+\w*)\s*=(.*)$',query)
	if m == None:
		print 'cannot parse expression, invalid variable name on left side of equation'
		return None,None

	return m.group(1),m.group(2)

#
#Checks whether the query 'QUERY' is a valid query in the current database
#ex. Database contains: a = 0, b = 0.
#    QUERY = 'a = 3; b = c * c'		-	QUERY uses the variable 'c' before it assigns a value to it -> QUERY is invalid.
#
#ex. QUERY = '12a = 3'			-	'12a' is an invalid variable name
#
#Always process subqueries from left to right
def check_if_valid(database, QUERY):

	db_variables = os.popen('ls '+database).readlines()	#reads all variable names from database

	for query in QUERY.split(";"):

		tokens = get_tokens(query)

		for token in tokens[1:]:			#checks if all tokens in the right side of the subquery exist in the database
			if not (token+'\n' in db_variables):
				print "'",token, "' not in database."
				return False

		left,right = get_left_right_expression(query)

		if left == None or right == None:
			print query, ' is not a valid expression.'
			return False
		try:
			for token in tokens[1:]:		
				f = open(database + token,'r')
				val = f.read().strip()
				f.close()
				exec(token + '=' + val)

			eval(right)
		except Exception as e:
			print 'An error occured while parsing the expression: ',QUERY
			print 'Error: ',e
			return False

		db_variables += [left+'\n']			#The variable on the left side of the equation may be created for the first time. Add it to the list of variables, so that the next subqueries that use it are valid.

	return True

#
#Set the variable 'var' in database 'database' to the value 'val'
#
def set_var(database,var,val):
	f = open(database+var,'w')
	f.write(str(val))
	f.close()

#
#Execute the query 'QUERY' over the database 'database'
#and return the result as an update string for the other databases: var1=val1; var2 = val2; var3 = val3; ...
#
def execute_local_query(database, QUERY):

	update_query_list = []	
	for query in QUERY.split(";"):					#for each subquery
		tokens = get_tokens(query)				#extract the tokens/variables concerned
		for token in tokens[1:]:				#for each token
			fil = open(database + token, 'r')
			exec(token + '=' + fil.read().strip())		#read its value and store it in runtime
			fil.close()
		left,right = get_left_right_expression(query)		#extract the left and right side of the subquery
		result = str(eval(right))				#evaluate the right side
		exec(left + '=' + result)				#store the result in the variable on the left side

		for token in tokens:					#construct the update query that needs to be sent to other nodes to update their databases
			set_var(database,token,result)
			update_query_list += [token + '=' + result]

	return ';'.join(update_query_list)				#the update query looks like: var1=val1; var2 = val2; var3 = val3; ...

#
#If the node's database doesn't exist, create it as a folder with the name <database>
#
def create_local_database(database):
	dirs = os.popen("ls -l | grep ^d | awk '{print $9}'").readlines()
	for d in dirs:
		if database[:-1] in d: return
	
	os.popen('mkdir '+database).close()

#
#Prints the database contents
#
def list_database_contents(database):
	dirs = os.popen("ls -l | grep ^d | awk '{print $9}'").readlines()

	ok = False
	for d in dirs:
		if database[:-1] in d: 
			ok = True

	if not ok:
		print "Database " ,database ," doesn't exist"
		sys.exit(1)

	entries = os.popen('ls ' +database).readlines()
	PHASE1_LOCKS = {}
	for entry in entries:
		entry = entry.strip()
		f = open(database+entry)
		content = f.read()
		f.close()
		PHASE1_LOCKS[entry] = False
		
		print entry,'=',content


	return PHASE1_LOCKS

