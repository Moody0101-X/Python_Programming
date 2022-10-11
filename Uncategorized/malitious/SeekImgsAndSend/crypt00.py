from string import *
from dataclasses import dataclass
from typing import Union
from datetime import datetime
from json import dumps, loads
import hashlib # hashlib.algorithms_guaranteed to know the algos that r there.


class crypt:
	"""a class to encrypt and decrypt data using string manipulation."""
	
	def __init__(self, data: Union[str, int] = None, seed: int =None):
		self.data = data
		self.seed = seed
		self.setup()
	def __repr__(self):
		return '<encrypted data/>'
	def __str__(self):
		return 'encrypted data'
	def crypt_00(self):
		data = self.data
		edata = []
		i = 0
		for _ in data:
			if i > len(data):
				i = 0
			edata.append(str(ord(_) + int(self.seed[i])))
		self.storeseed(key=edata)
		return ''.join(edata)
	def decrypt_00(self):
		seed, key = str(loads(open("seeds.json", "r").read())['seed']), loads(open("seeds.json", "r").read())['key']
		res = ''
		j = 0
		for i in key:
			if  j > len(key):
				j = 0
			res += chr((int(i) - int(seed[j])))
		return res
	 	
	@property
	def getseed(self):
		return (self.seed, self.seedlength)
	def setup(self):
		if not self.seed:
			self.seed = str(datetime.now())[-6:]
		if isinstance(self.seed, int):
			self.seed = str(self.seed)
		if isinstance(self.data, int):
			self.data = str(self.data)
		self.seedlength = len(self.seed)
	def storeseed(self, key):
		data = dumps({'seed': self.seed, 'key':key}, indent=4)
		with open('seeds.json', "w+") as db:
			db.write(data)
			return 0
		return 1

def test():
	data = crypt() 
	data00 = data.decrypt_00()
	print(data00)
test()
