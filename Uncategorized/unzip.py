"""
Cli to unzip archives using python

-> Author: Moody0101
-> repo: github.com/Moody0101/python0101

usage:	unzip fileName
"""

from shutil import unpack_archive
from sys import argv
from os import path, scandir
from colorama import Fore as F

def UNZIP():
	if len(argv) == 1:
		print(__doc__)
	elif len(argv) > 1:
		if argv[1] == "--help" or argv[1] == "-h":
			print(__doc__)
		else:
			decompress(argv[1])		

def re_(spec: str):
	if '\\' in spec: return '\\'
	elif '/' in spec: return '/'
	else:
		return " "

def decompress(dir_, extractdir=None):

	if not path.exists(dir_): print("The specified file does not exist!")

	else:
		filename = dir_.split(
		re_(dir_)
		)[-1].split(".")[0]
		
		if not extractdir: extractdir = f'.{re_(dir_)}{filename}'

		else:
			extractdir = path.join(dir_.split(re_(dir_))[:-1])
		try:
			unpack_archive(dir_, extractdir)
			for i in scandir(extractdir):
				print(f"{F.YELLOW}unpacked {i.name}")
				print(f"{F.GREEN}unpacked {filename}.zip {F.RESET}")
			
		except Exception as e:
			print(f"{F.RED}something went wrong unpacking {filename}.zip\n", e)
		return filename

UNZIP()