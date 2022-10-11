from Crypto.Cipher.AES import new
from Crypto.Cipher import AES
import random

class crypto__:
	def __init__(self, data: str, MODE=None):

		self.data = data
		self.MODE = MODE
		self.paddedData = self.data + (16 - len(self.data) % 16) * "}"
		if self.MODE == None:
			self.MODE =  AES.MODE_CBC
			print("initialized the mode to => CBC Mode")
			print(self.paddedData)
			print(len(self.paddedData))
	def encrypt_(self):	
		self.KEY = ''.join([chr(random.randint(0, 100)) for i in range(16)]).encode()
		return (self.KEY, new(self.KEY, self.MODE).encrypt(self.paddedData.encode()))
		
	def decrypt_(self, KEY, CIPHER):
		return new(KEY, self.MODE).decrypt(CIPHER)


obj = crypto__("HELLO")
DATA = obj.encrypt_()
obj.decrypt_(DATA[0], DATA[1])
