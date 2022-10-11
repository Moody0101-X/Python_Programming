from subprocess import run
from colorama import Fore
"""
Personal wifi scanner
"""
data = run("netsh wlan show networks".split(' '),  shell=True, capture_output=True, encoding="utf-8").stdout.split("SSID")

def scan(print_=False):
	Names = []
	for i in data[1::1]:
		if print_:
			print(f"{Fore.BLUE}[*]SSID => {Fore.GREEN}{i.split(':')[1].split(' ')[1].strip()}{Fore.RESET}")
			print(f"{Fore.YELLOW}\t[!]Network type => {Fore.LIGHTYELLOW_EX}{i.split(':')[2].split(' ')[1].strip()}{Fore.RESET}")
			print(f"{Fore.YELLOW}\t[!]Authentication => {Fore.LIGHTYELLOW_EX}{i.split(':')[3].split(' ')[1].strip()}{Fore.RESET}")
			print(f"{Fore.YELLOW}\t[!]Encryption => {Fore.LIGHTYELLOW_EX}{i.split(':')[4].split(' ')[1].strip()}{Fore.RESET}")
			print('\n')
		Names.append(i.split(':')[1].split(' ')[1].strip())
	return Names

scan()

