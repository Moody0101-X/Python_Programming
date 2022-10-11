from sys import argv
from os import path, scandir
from time import sleep
"""
little script to automate the painful tasks..
"""
def makestatements(dataPath, outputFile):
	if path.exists(dataPath):
		stmnts = [f"import {str(i.name.split('.')[0])} from \'{argv[1]}/{i.name}\';" for i in scandir(dataPath) if not str(i.name.split('.')[0]) == " "]
		with open(outputFile, 'w+') as f:
			for _ in stmnts:
				f.write(_)
				f.write('\n')

	else:
		print("seemes like the file you just specified does not exist! try again with a valid path.")
		
def main():
	makestatements(argv[1], argv[2])
if __name__ == '__main__':
	main()