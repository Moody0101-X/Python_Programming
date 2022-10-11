from random import choice
from colorama import Fore
from time import sleep

prefixes = [
'social', 'marketing', 'hello', 'contact', 'support', 'info', 'press',
'media', 'team', 'sales', 'enquiries', 'help', 'business', 'service',
'career', 'community', 'opportunities', 'accelerate',"hi"
]

with open("Names.csv", "r") as f:
	print(f"{Fore.RED}LOADING...")
	filedata = f.readlines()

def generateEmails():
	print(f"scrapped {len(filedata)} total emails")
	for i in filedata[1:]:
		sleep(0.5)
		print(f"{Fore.LIGHTYELLOW_EX}[*] => {i.strip()}@{choice(prefixes)}.com")

generateEmails()
print(Fore.RESET)