from subprocess import run, PIPE
from colorama import Fore as F
from scan import scan





class Scanner:
	
	def getStoredWifis():
		firstPrompt = run("netsh wlan show profiles", shell=True, capture_output=True, encoding="utf-8").stdout
		return [i.split('\n')[0].strip() for i in [i for i in firstPrompt.split('-------------')[-1].split(":")][1:-1]]

	def getpassword(ESSID):
		cmd = f"netsh wlan show profile \"{ESSID}\" key=clear"
		prompt = run(cmd, shell=True, capture_output=True, encoding="utf-8").stdout.split("Security key")[1].strip().split("Cost settings")[0].strip()[1:].strip().split(":")[-1]
		print(f'{F.GREEN}[*] {F.LIGHTRED_EX}{ESSID} :{F.LIGHTYELLOW_EX}', prompt)
	
	# for ESSID in getStoredWifis():
	# 	getpassword(ESSID)
	for ESSID in scan():
		if ESSID in getStoredWifis():
			getpassword(ESSID)
		else:
			print(f"{F.RED}[!] {ESSID} Not found")
Scanner()
print(F.RESET)