

from string import ascii_letters


def char(i):
	for _ in ascii_letters:
		if ord(_) == i:
			return _
		else:
			pass


def tobytes(s):
	if len(s) == 1:
		return ord(s).to_bytes(2, 'little')



def splitBinary(bits: str) -> str:
	x, i = 0, 0
	s = ""
	while x < len(bits) - 1:
		while i < len(bits)/7 and i+x != len(bits):
			s += bits[x+i]
			i += 1
		x += 7
		s += " "
		i = 0
	return s.split(" ")[:-1]


def chartob10(char):
	if len(char) == 1:
		return int(ord(char))
	else:
		return ''.join([str(ord(i)) for i in char])

def b2tob10(a: str) -> int:

	base = 0
	num = 0
	for i in a[::-1]:
		if int(i) != 0:
			num += 2**base
			base += 1
		else:
			base += 1
	return num

def b10tob2(a: int) -> str:
	binary = ""
	remainder = 0
	while a != 0:
		remainder = a%2
		binary += str(remainder)
		a //=2
	return binary[::-1]

print(chartob10("HELLO"))
print(splitBinary(charto("HELLO")))
for i in splitBinary(chartob2("HELLO")):
	print(i)