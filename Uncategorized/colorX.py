from colorama import Fore as f
if __name__ == '__main__':
	for c in [i for i in dir(f) if not i.startswith("__")]:
		print(f"{f} I Am black {f.eval(c)} and I am YELLOW {f.RESET}")