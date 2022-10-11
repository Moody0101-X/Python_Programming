
"""
does dir(class) return a list with all the content of the class!?
this is what I am testing..
"""
# making a class firstly
from random import randint
from pprint import pprint
from dirfunc import getDIR
import os
class MinecrafPlayer:
	def __init__(self,
	name: str,
	level: int,
	items: list,
	position: tuple,
	worldseed: int
	) -> None:
		self.name = name
		self.level = level
		self.items = items
		self.position = position
		self.worldseed = worldseed
		self.stats = self.getstats
	def getname(self) -> str: return self.name
	def getlvl(self) -> int: return self.level
	def getitems(self) -> list: return self.items
	def getpos(self) -> tuple: return self.position
	def getseed(self) -> int: return self.worldseed
	@property
	def getstats(self) -> dict: return {"Name" : self.getname(), "Level" : self.getlvl(), "items" : self.getitems(), "positon" : self.getpos(), "seed" : self.getseed()}
	def present(self, all: bool = False) -> None: 
		if all == False:
			print(f"""
----------------------------------------------------
*	name: {player.name}
*---------------------------------------------------
----------------------------------------------------
*	level: {player.level}
*---------------------------------------------------
----------------------------------------------------
*	position: {player.position}
*---------------------------------------------------
""")
		else:
			print("Working on it")
#Now testing :)



if __name__ == '__main__':
	player = MinecrafPlayer('Moody',
	20,
	['pickaxe', 'beef, crafting table']
	, (100, 28, 10), randint(10000, 99999))
	# pprint(player.getstats)
	print("dir of instance")
	print([i for i in dir(player) if not i.startswith('__')])
	print("dir of class")
	print([i for i in dir(MinecrafPlayer) if not i.startswith('__')])
	"""
	by filtering the dunder funcs
	dir(player) returns:
		['getitems', 'getlvl', 'getname', 'getpos', 
		'getseed', 'getstats', 'items', 'level',
		'name', 'position', 'present', 'stats', 'worldseed']
	dir(MinecrafPlayer) returns:
		['getitems', 
		'getlvl', 
		'getname', 
		'getpos', 
		'getseed',
		 'getstats',
		  'present']
	to diffrentiate before initializing the class, the dir has only the the functions
	but when we make the instance, everything gets pushed into that list.
	that makes perfect sense.
	"""
	print("""
----**100
""")
	print("Os lib data")
	pprint(getDIR(os.path))
	