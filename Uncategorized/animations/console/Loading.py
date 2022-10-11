from sys import stdout
from colors import *
from time import sleep
from math import tan

for _ in ["-", "\\", "|", "/"]*20:
	stdout.write(f"{YELLOW} Loading {_}{RESET} \r")
	sleep(.2)
