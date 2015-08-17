#!/usr/bin/env python
import sys 

""" myDB.py: creates an in-memory database object that receives data or 
transaction commands via stdin, performs the command, and outputs the 
response via stdout. 

Example:
	Run myDB.py by passing in a file of commands. 
	$ python myDB.py < test.txt

Example: 
	Another option is to run myDB.py, then type in via the terminal
	various commands.

	$ python myDB.py
	$ BEGIN
	$ SET a 50
	$ GET a 50
	$ 50 

"""
__author__ = "Jade Hua"
__email__ = "jadehua@berkeley.edu"

class DB: 
	""" DB represents a database that holds data and can have many 
	transactions performing on the data. 

	It holds a stack of transactions with the committed transactions being at 
	the bottom of the stack. The most recent transaction is at the top of 
	the stack. The DB also holds info on the counts of the values it holds. 
	The occurences of the values are also stored in a stack, with the
	committed counts at the bottom of the stack, and the most recent counts
	at the top of the counts stack. 
	
	"""
	# Setting constants for data commands
	(SET, GET, UNSET, NUMEQUALTO, END) = ("SET", "GET", "UNSET", "NUMEQUALTO", "END")
	# Setting constants for transaction commands 
	(BEGIN, ROLLBACK, COMMIT) = ("BEGIN", "ROLLBACK", "COMMIT")

	def __init__(self): 
		""" Intializes a database object

		:xcts			stack representing transactions
		:valueCounts	stack representing counts of the values 
		:inTransac		Boolean whether there is a transaction occurring
		:currXct		pointer to the current transaction in xcts 
		:currValCount	pointer to the current value count in valueCounts 	

		"""
		self.xcts = [] 
		self.xcts.append({}) # putting committed at the bottom of stack
		self.valueCounts = [{}]
		self.inTransac = False 
		self.currXct = {}
		self.currValCount = {}

	def set(self, name, value): 
		""" Sets name's value to value in the database. 
		
		This method also checks whether setting this current transaction's
		name to a new value will replace an older value that already is mapped
		to this name. If so, it decrements the count of the older value from
		valueCounts.

		If the new value to be set increases the count of the value in the 
		database, then the method will increase the count of the new value
		from valueCounts or set it to 1 if its the first time this value
		has been set.

		:param name:	the name of the variable to be set
		:param value:	the value to be set to name 

		"""
		if name in self.currXct:
			oldVal = self.currXct[name] 
			if oldVal != value and oldVal in self.currValCount: 
				self.currValCount[oldVal] -= 1 

		self.currXct[name] = value

		if value in self.currValCount: 
			self.currValCount[value] += 1
		else:
			self.currValCount[value] = 1

	def get(self, name): 
		""" Gets the corresponding value for this name in the database 
		
		If the name does not exist as a key in the database, the method will
		output NULL, else it will output the value. 

		:param name:	the name of the variable to get

		"""
		value = None 
		if name in self.currXct: 
			value = self.currXct[name]

		if value is None: 
			sys.stdout.write('NULL\n')
		else: 
			sys.stdout.write(str(value) + '\n')

	def unset(self, name):
		""" Sets name's value to None in the database 
		
		This method also decreases the count for the value that was unset 
		in the valueCounts. 

		:param name:	the name of the variable to be set

		""" 
		oldVal = self.currXct[name]

		self.currXct[name] = None			

		if oldVal is not None: 
			self.currValCount[oldVal] -= 1
			
	def numEqualTo(self,value):
		""" Finds the number of occurrences of this value in the database 
		
		This method checks whether this value exists in the database and 
		outputs its count if it exists, or 0 if it doesn't not exist. 

		:param value:	look for the count of value

		"""
		if value in self.currValCount: 
			sys.stdout.write(str(self.currValCount[value]))
			sys.stdout.write('\n') 
		else:
			sys.stdout.write(str(0))
			sys.stdout.write('\n') 

	def end(self): 
		""" Exists out of the program """
		sys.exit() 

	def begin(self): 
		""" Begins a new transaction
		
		This method creates a new transaction that is a copy of the most recent 
		transaction on top of the stack of transactions. Then, it puts the new 
		transaction on top the stack by appending to the end of the xcts list. 

		This method also creates a new value counts that is a copy of the most
		recent value count on top the stack of value counts. Then, it puts the
		new value count on top the stack by appending it to the end of the
		valueCounts list. 

		Begin also sets inTransac to True since it's starting a new transaction. 

		"""
		self.inTransac = True 
		xct = self.xcts[-1].copy()
		self.xcts.append(xct)

		valCount = self.valueCounts[-1].copy()
		self.valueCounts.append(valCount)

	def rollback(self):
		""" Reverts to the previous transaction. 
		
		This method first checks if the database has any ongoing transactions. 
		If there are no transactions, rollback will output NO TRANSACTION. 

		If there are transactions, rollback will pop off the top transaction
		on the xcts stack, and the value counts stack. This will revert back 
		to the previous transaction and value count.

		If popping off the top transaction results no more transactions, 
		then the method will set inTransac to False. 

		"""
		if not self.inTransac:
			sys.stdout.write("NO TRANSACTION\n")

		else: 
			self.xcts.pop()
			self.valueCounts.pop()
			if not self.xcts: # no more transactions
				self.inTransac = False

	def commit(self):
		""" Commits all the ongoing transactions.
		
		This method first checks if the database has any ongoing transactions. 
		If there are no transactions, commit will output NO TRANSACTION. 

		If there are transactions, commit will set the transaction stack to
		the top of the stack. Now the transaction stack will only have one 
		element which represents all the changes made in the transactions. 

		After committing, the method sets inTransac to False since there are
		no more ongoing transactions. 

		"""
		if not self.inTransac: 
			sys.stdout.write("NO TRANSACTION\n")
		else: 
			self.xcts = [self.currXct.copy()]
			self.valueCounts = [self.currValCount.copy()]

		self.inTransac = False 

	def performCommand(self, line):
		""" Interprets and performs a command based on stdin. 
		
		This method takes in a line from stdin and splits it by spaces into
		a list of instructions. It takes the first element of the instruction
		and compares it to the various commands declared as global variables
		in a DB. The method then calls the necessary method of the DB to 
		execute this command, and extracts the necessary parameters from the
		instruction list to pass in. 

		The method also figures out what the current transaction and current
		value count is for data commands. If the database is in transactions,
		the current transaction and value count is the top of its respective 
		stacks. If not, the two are its two committed forms at the bottom of
		their respective stacks. 

		"""
		instruc = line.strip().split(" ")
		command = instruc[0]

		if not self.inTransac: # not in transaction
			self.currXct = self.xcts[0]
			self.currValCount = self.valueCounts[0]
		else:
			self.currXct = self.xcts[-1]
			self.currValCount = self.valueCounts[-1]
			
		if command == db.BEGIN: 
			self.begin() 
		elif command == db.ROLLBACK: 
			self.rollback()
		elif command == db.COMMIT:
			self.commit()
		elif command == db.SET: 
			(name, value) = (instruc[1], instruc[2])
			self.set(name, value)
		elif command == db.GET: 
			name = (instruc[1])
			self.get(name)
		elif command == db.UNSET: 
			name = instruc[1]
			self.unset(name)
		elif command == db.NUMEQUALTO: 
			value = instruc[1]
			self.numEqualTo(value)
		elif command == db.END:
			self.end() 
		else: 
			sys.stdout.write("Invalid command. Try again.\n")

if __name__ == "__main__":
	line = sys.stdin.readline()
	db = DB() 
	while(line): 
		db.performCommand(line)
		line = sys.stdin.readline()
