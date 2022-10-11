"""
Font downloader using the google funt api, and the Dafont api.
"""

from os import system, getcwd
try:
	from colorama import Fore
except:
	print("Installing colorama!!")
	system("pip install colorama")
try:
	from requests import get
except:
	print("Installing requests!!")
	system("pip install requests")

from sys import argv

fontName = input(f"{Fore.YELLOW}Enter the font name : ")
urls = [
f"https://fonts.google.com/download?family={fontName}",
f"https://dl.dafont.com/dl/?f={fontName}"
]
reqs = [get(urls[0]), get(urls[1])]
APIS = ["Google Font", "Dafont"]
for i, res in enumerate(reqs):
	
	if int(res.status_code) == 200:
		if len(res.content) < 200:
			print(f"{Fore.RED}[*] {Fore.MAGENTA}{fontName} was not fount in {APIS[i]}")
		else:
			print(f"""{Fore.YELLOW}
File name: {Fore.LIGHTYELLOW_EX}{fontName}.zip
{Fore.YELLOW}size:  {Fore.LIGHTYELLOW_EX}{len(res.content)} Bytes
{Fore.YELLOW}provider: {Fore.LIGHTYELLOW_EX}google fonts
{Fore.YELLOW}Download directory: {Fore.LIGHTYELLOW_EX}{getcwd()}
{Fore.YELLOW}Api: {Fore.LIGHTYELLOW_EX}{APIS[i]}
""")
			with open(f"{fontName}.zip", "wb") as f:
				print(f"{Fore.BLUE}downloading.. {fontName}")
				f.write(res.content)
			print(f"{Fore.GREEN}successfully donwloaded {fontName}")
			break
	else:
		print(f"{Fore.RED}[*] {Fore.MAGENTA}{fontName} was not fount in {APIS[i]}")

print(Fore.RESET)