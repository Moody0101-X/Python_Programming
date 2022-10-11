from qrcode import make
from dataclasses import dataclass
from colors import *
from base64 import b64encode

class _Qrmaker:
	
	def __init__(self, content, ext: str = 'PNG', output:str = 'Output.png'):
		self.content = content
		self.ext = ext
		self.output = output
		try:
			self.img = make(self.content)
			self.img.save(self.output, self.ext)
		except:
			print('something went wrong')

if __name__ == '__main__':
	print(....__init__)
	qrcodemessage = input(f"{ YELLOW } what would you like to make as qr code: {RESET}")
	name = b64encode(qrcodemessage[:4].encode()).decode() + ".png"
	try:
		_Qrmaker(qrcodemessage, output=name)
		print(f"{GREEN} your QRcode was made successfully! \n name: {name} \n path: .")
	except Exception as e:
		print(f"{RED} e {RESET}")