from zipfile import ZipFile, ZIP_DEFLATED
from os import path, getcwd, scandir, chdir, mkdir
from colorama import Fore as F
from time import sleep
from shutil import make_archive, copy

class Packing:
	def __init__(self, Name):
		self.Name = Name
		self.dir = self.Name.split('.')[0]
		if not path.exists(self.Name):
			pass			
		else:
			self.getFileList()
			
	def pack(self, path_: str | list = None):
		if path_:
			with ZipFile(self.Name, 'w', compression=ZIP_DEFLATED) as Zip:
				if isinstance(path_, str):
					if path.exists(path_):
						Zip.write(self.Name)
						print(f"{F.LIGHTYELLOW_EX}[*] Zipped {path_} to {self.Name}")
						sleep(.3)
					else:
						print(f"{F.RED}{path}[!] Does not exist")
						sleep(.3)
				elif isinstance(path_, list):
					for dir_ in path_:
						self.pack(dir_)
					print(f"{F.GREEN}[*] zipped Everythin!")
		else:
			pack([i.name for i in scandir(getcwd()) if not i.name.startswith("__")])

	def unpack(self, files: str | list = None):
		if not path.exists(self.Name):
			print(f"{F.RED}[!] Enable to unpack a file that does not exits")
			exit(1)
		with ZipFile(self.Name, 'r') as Zip:
			if not files:
				Zip.extractall(self.dir)
				print(f"""{F.LIGHTYELLOW_EX}[*] extracted {self.Name} to {self.dir}""")
			else:
				if not path.exists(self.dir):
					mkdir(self.dir)
				chdir(self.dir)
				if isinstance(files, list):
					for _ in files:
						self.unpack(str(_))
				elif isinstance(files, str):
					if files in self.FileList:
						Zip.extract(files)
						print(f"""{F.LIGHTYELLOW_EX}[*] extracted {files} to {self.dir}""")
					else:
						print(f"""{F.RED}[!] {files} does not exist in {self.Name}""")

	def getFileList(self) -> list:
		with ZipFile(self.Name, 'r') as Z:
			self.FileList = Z.namelist()


def main():

	files = [i.name for i in scandir(getcwd()) if i.name.endswith('py')]
	size = 0
	for i in files:
		copy(i, 'files')
		with open(i, "rb") as f:
			size += len(f.read())
			print(size)

	print(f"{F.LIGHTGREEN_EX}BeF compression => {size} Byte")
	Zipper = Packing("PythonFiles.zip")
	Zipper.unpack()
	# with open('PythonFiles.zip', 'rb') as f:
	# 	print(f"{F.LIGHTGREEN_EX}After compression => ", len(f.read()), " Byte")
	
if __name__ == '__main__':
	main()
	print(F.RESET)