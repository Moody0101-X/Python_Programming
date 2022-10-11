"""
Using Multiple processes in python
"""

from multiprocessing import Process, freeze_support
from threading import Thread
from time import time
from sys import stdout, argv
# class UsePocess:
# 	def __init__(self, list, Processes: list[Process]) -> None:
# 		self.Processes = Processes
# 		for i in self.Processes:
# 			i.start()
# 			i.join()

#

def calculate(n) -> int:
	if n == 1:
		pass
	else:
		n += 1
	return n + 1

def LIST(n) -> list[int]:
	X = []
	for i in range(n):
		X.append(calculate(i))
	return X

def Normal(n) -> int:
	start = time()
	for c in [1000 + i*1000 for i in range(n)]:
		L = LIST(c)
	return time() - start

def Multi(n):
	start = time()
	P = [Process(target=LIST, args=(1000 + i * 1000, )) for i in range(n)]
	for i in P:
		i.start()
	for i in P:
		i.join()
	return time() - start
def Thread_(n):
	start = time()
	P = [Thread(target=LIST, args=(1000 + i * 1000, )) for i in range(n)]

	for i in P:
		i.start()
	for i in P:
		i.join()
	return time() - start
def compare(n) -> tuple:
	return (Normal(n), Multi(n), Thread_(n))

if __name__ == '__main__':
	freeze_support()
	n = int(argv[1])
	stats = compare(n)
	stdout.write(f"Normal: {stats[0]}\n")
	stdout.write(f"Process: {stats[1]}\n")
	stdout.write(f"Thread: {stats[2]}\n")



