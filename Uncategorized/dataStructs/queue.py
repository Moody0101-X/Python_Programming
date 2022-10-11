"""
queue implementaion.

the second and more firm one.

"""
from collections import deque

class Queue:
	def __init__(self, items : list = []):
		self.queue = deque()
		self.items = items
		if len(self.items) > 0:
			for _ in self.items:
				self.pushRight(_)
	def __len__(self):
		return len(self.queue)
	def __str__(self):
		return str(list(self.queue))
	def __repr__(self):
		return f"<<queue object>>"
	def __list__(self):
		return list(self.queue)		  
	def pushLeft(self, data):
		self.queue.appendleft(data)
	def pushRight(self, data):
		self.queue.append(data)
	def insert(self, data, i):
		self.queue.insert(data, i)
	def pop(self):
		self.queue.pop()
	def popleft(self):
		self.queue.popleft()
	def index(self, item):
		try:
			return self.queue.index(item)
		except:
			print("does not exist! ")
	def remove(self, index):
		self.queue.remove(index)
	def reverse(self):
		self.queue.reverse()
	def rotate(self):
		self.queue.rotate()
	def copy(self):
		return self.queue.copy()
