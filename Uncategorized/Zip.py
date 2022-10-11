"""
Cli to zip archives using python

-> Author: Moody0101
-> repo: github.com/Moody0101/python0101

usage:	zip filepath/Filename [format=zip]
"""

from shutil import make_archive, unpack_archive
from sys import argv
from os import path, chdir, mkdir, scandir
from colorama import Fore as F


def ZIP():
	if len(argv) == 1:
		print(__doc__)
	elif len(argv) > 1:
		if argv[1] == "--help" or argv[1] == "-h":
			print(__doc__)
		else:
			if len(argv) == 2:
				compress(argv[1])
			elif len(argv) == 3:
				compress(argv[1], argv[2])
			else:
				print(__doc__)
	else:
		print(__doc__)

def compress(dir_, Mode="zip"):
	filename = dir_.split(
	'\\' if '\\' in dir_ else '/'
	)[-1]
	if path.exists(dir_): 
		chdir(dir_)
		for i in scandir(dir_):
			print(f"{F.YELLOW}packing {i.name}...")
		make_archive(filename, format=Mode, base_dir=dir_)
		print(f"{F.GREEN}Finished packing {filename}{F.RESET}")
		return filename
	else: print("specified file does not exist!!")



ZIP()