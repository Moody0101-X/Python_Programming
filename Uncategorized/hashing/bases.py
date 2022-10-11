"""

a Module to encode and decode to base64 and other encodings using the data 
provided from the command line or using data from another script. 

made it because it is not cool to keep doing unnecessary stuff
that module wrapps the most useful from base64 module.

"""



from base64 import *
from typing import Union
from colorama import Fore


class base64:

	def __init__(self, data: Union[bytes, str]):
		self.data = data
	def _encode(self) -> str:
		if isinstance(self.data, bytes):
			return b64encode(self.data).decode()
		else:
			return b64encode(self.data.encode()).decode()
	def _decode(self) -> str:
		if isinstance(self.data, bytes):	
			return b64decode(self.data).decode()
		else:
			return b64decode(self.data.encode()).decode()
	def __str__(self):
		return str(self.__class__.__name__)


class base32:
	def __init__(self, data: Union[bytes, str]):
		self.data = data
	def _encode(self):
		if isinstance(self.data, bytes):
			return b32encode(self.data).decode()
		else:
			return b32encode(self.data.encode()).decode()

	def _decode(self):
		if isinstance(self.data, bytes):
			return b32decode(self.data).decode()
		else:
			return b32decode(self.data.encode()).decode()

	def __str__(self):
		return str(self.__class__.__name__)


class base16:
	def __init__(self, data: Union[bytes, str]):
		self.data = data
	def _encode(self):
		if isinstance(self.data, bytes):
			return b16encode(self.data).decode()
		else:
			return b16encode(self.data.encode()).decode()

	def _decode(self):	
		if isinstance(self.data, bytes):
			return b16decode(self.data).decode()
		else:
			return b16decode(self.data.encode()).decode()

	def __str__(self):
		return str(self.__class__.__name__)


class a85:

	def __init__(self, data: Union[bytes, str]):
		self.data = data.decod
	def _encode(self):
		if isinstance(self.data, bytes):
			return a85encode(self.data).decode()
		else:
			return a85encode(self.data.encode()).decode()

	def _decode(self):
		if isinstance(self.data, bytes):
			return a85decode(self.data).decode()
		else:
			return a85decode(self.data.encode()).decode()

	def __str__(self):
		return str(self.__class__.__name__)

class b85:
	def __init__(self, data: Union[bytes, str]):
		self.data = data
	def _encode(self):
		if isinstance(self.data, bytes):
			return b85encode(self.data).decode()
		else:
			return b85encode(self.data.encode()).decode()

	def _decode(self):	
		if isinstance(self.data, bytes):
			return b85decode(self.data).decode()
		else:
			return b85decode(self.data.encode()).decode()

	def __str__(self):
		return str(self.__class__.__name__)
# testing

if __name__ == '__main__':
	for i in [base16, b85, base32, base64]:
		ins = i('hello')
		print(f"{Fore.YELLOW}{str(ins)} => {Fore.CYAN}", f"{ins._encode()}{Fore.WHITE}")
