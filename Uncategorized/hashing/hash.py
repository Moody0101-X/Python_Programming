from hashlib import *
from json import dumps, loads
from pprint import pprint
from os import path
from hmac import compare_digest
import hashlib

# Heldane Display Web is a nice font
"""
Classes: [db, login, signin]

[login, signin] => depend on db to either sign in to the db or login to 
verify the password.

login => loads the database and checks if the value does exist in the db.
signin => loads data to the data base.

this is the basic, but you can make the same functionality with
alot of other algorithms like:
	you can check availableAlgos using:
		print(algorithms_available)
	to see there doc:
		print(Name.__doc__)
	then there would a documentation that explains how to use the algorithm.
I think that I will use this schema for future projects.
"""
# basic Authentication using sha256

class DbDoesNotExistErr(Exception):
	pass

class db:
	def __init__(self, data: dict={}):
		self.data = data
		self.db = 'db.json'
		if self.data != {}:
			self.dump()
		else:
			print('initializing dataBase')
	def dump(self):
		with open(self.db, 'w+') as f:
			f.write(dumps(self.data, indent=4))
			pprint(self.data)
			print("dumped into db.")
	def loaddb(self):
		if path.exists(self.db):
			return loads(open(self.db).read())
		else:
			DbDoesNotExistErr("Db does not exist!!")


class login:
	def __init__(self, userName, password):
		self.userName = userName
		self.password = password
		self.dataBase = db()
		if self.verify():
			print(f'success!!! {self.userName}')
		else:
			print('fail')
	def verify(self):
		hash0 = sha256(f"{self.password}".encode()).hexdigest()
		return compare_digest(self.dataBase.loaddb()['password'], hash0)



class signIn:
	def __init__(self, userName: str, password: str) -> None:
		self.userName = userName
		self.password = password
	def sign(self) -> bool:
		database = db(self.getData)
		print('Signed in successfully!!')
		return True
	@property
	def getData(self):
		return {
		"user": self.userName,
		"password": sha256(f"{self.password}".encode()).hexdigest()
		}


login('Az123', '123Huaeeedcssd')
