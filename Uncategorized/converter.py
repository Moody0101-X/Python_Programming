
from typing import Union
from string import (printable) 
import builtins


class char:
	"""
	this class converts Binary and Integer to characters, so basically this is the reverse of
	the ord built-in function
	"""
	def __init__(self, character):
		self.character = character
		
	def bintochar(self) -> str:
		if isinstance(self.character, int):
			return self.check(self.character)
		else:
			return self.check(converter().binaryToInteger(self.character))
	def integerTochar(self):
		return self.check(self.character)
	def check(self, i: int):
		for _ in printable:
			if ord(_) == i:
				return _
			else:
				pass
		return False


class converter:
	"""
	Python base class for python self.
	functions: integersToBinary, tochar, bintochar, stringTobin, chartobin
	"""
	def integersToBinary(self, i: int) -> str:
		binary = ""
		remainder = 0
		while i != 0:
			remainder = i%2
			binary += str(remainder)
			i //=2
		return binary[::-1]
	
	def binaryToInteger(self, b: Union[str, int]) -> int:
		if isinstance(b, int):
			b = str(b)
		base = 0
		num = 0
		for i in b[::-1]:
			if int(i) != 0:
				num += 2**base
				base += 1
			else:
				base += 1
		return num

	def tochar(self, i: Union[int, str, list]) -> str:
		if isinstance(i, str):
			i = int(i)
		elif isinstance(i, list):
			return [char(self.integersToBinary(x)).bintochar() for x in i]
		return char(self.integersToBinary(i)).bintochar()



	def bintochar(self, b: Union[str, int]) -> str:
		if b:
			return self.tochar(self.binaryToInteger(b))


	def stringTobin(self, s):
		_string = ""
		for _ in s:
			_string += self.chartobin(_)
			_string += " "
			# _string = _string.strip()
		return _string


	def chartobin(self, c: str):
		return self.integersToBinary(int(ord(c)))


class test1(converter, char):
	def __init__(self):
		super().__init__(character=None)
		self.test_char_int()
		self.test_bin_int()
	def test_bin_int(self):
		tenasbinary = self.integersToBinary(10)
		tenasint = self.binaryToInteger(tenasbinary)
		print(tenasbinary == self.integersToBinary(tenasint))
		print('It works!')
	def test_char_int(self):
		self.character = 44
		print(ord(self.integerTochar()) == 44)
		print("it works!!")
class MainConverter(converter, char):
	"""
	Int converters:
		integer to character => intToChar
		integer to binary => intToBin
	Bin converters:
		Bin to char => _binToChar
		Bin to integer => binToInt
	char converters:
		char to integer => _charToInt
		char to bin => _chartobin
	String converter:
		string to bin => _strToBin
	"""
	def intToChar(self,integer: int) -> str:
		return character(integer).integerTochar()
	def intToBin(self,i: int) -> str:
		return self.integersToBinary(i)
	def _binToInt(self,_bin: Union[int, str]) -> int:
		return self.binaryToInteger(_bin)
	def _binToChar(self,_bin) -> str:
		self.character = _bin
		return self.bintochar(self.character)
	def _charToInt(self,ch: str) -> int:
		return ord(ch)
	def _chartobin(self, ch: str) -> str:
		return self.intToBin(self._charToInt(ch))
	def stringTobin(self, string: Union[str,list]) -> str:
		for c in string:
			print(self._chartobin(c))
		return [self._chartobin(i) for i in string]
	def _binToString(self, _bin: Union[str, list]):
		if isinstance(_bin, list):
			return "".join([self._binToChar(i) for i in _bin])

class test2(MainConverter):
	def __init__(self):
		super(MainConverter).__init__()
		bin_ = ["0101101", "1011010"]
		
		ar = []
		for i in bin_:
			innt = self._binToInt(i)
			ar.append(innt)
		print(str(bytearray(ar).decode()))

"""
----------------------------------------------------------------------------------------------------------------
Notes:

Mainconverter implements char class and converter class, so don't change anything in either classes.

1 => 1
0
1 => 2**3
1 => 2**4
0
1 => 2**6
0

----------------------------------------------------------------------------------------------------------------
"""
if __name__ == '__main__':
	test2()
	print(converter().binaryToInteger(1011010))
	print(dir(builtins.bytearray([66])))
	
