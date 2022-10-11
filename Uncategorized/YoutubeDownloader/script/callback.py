from pafy import new
from os import system
try:
	from colors import *
except:
	try:
		from colorama import Fore
		BLACK = Fore.BLACK
		BLUE = Fore.BLUE
		CYAN = Fore.CYAN
		GREEN = Fore.GREEN
		LIGHTBLACK_EX = Fore.LIGHTBLACK_EX
		LIGHTBLUE_EX = Fore.LIGHTBLUE_EX
		LIGHTCYAN_EX = Fore.LIGHTCYAN_EX
		LIGHTGREEN_EX = Fore.LIGHTGREEN_EX
		LIGHTMAGENTA_EX = Fore.LIGHTMAGENTA_EX
		LIGHTRED_EX = Fore.LIGHTRED_EX
		LIGHTWHITE_EX = Fore.LIGHTWHITE_EX
		LIGHTYELLOW_EX = Fore.LIGHTYELLOW_EX
		MAGENTA = Fore.MAGENTA
		RED = Fore.RED
		RESET = Fore.RESET
		WHITE = Fore.WHITE
		YELLOW = Fore.YELLOW

	except:
		print("install colorama Using pip install colorama!")




def call(total, recvd, ratio, rate, eta):
	#{YELLOW} {total}
	print(f"TOTAL: {total} {BLUE}  [::] =>  {recvd} ", end="\r")


def Donwload(_id):
	print(f"{YELLOW} DOWNLOADING {new(_id).getbestvideo().title} {BLUE}:) {RESET}")
	new(_id).getbestvideo().download(callback=ConsoleProgress())
	print(f"{GREEN} Downloaded {RESET}")
try:
	Donwload("cF8B1a36Plc")
except KeyboardInterrupt as e:
	print(f"{RED}\n")
	print(f" Key interruption :( EXITING {RESET}")	
