from urllib import request
from pprint import pprint
from json import dumps, loads

"""
testing the dirfunc premise on a whole module/library
it seems like 
"""

def getDIR(module, data={}):
	for _ in dir(module):
		if _.startswith('_'): pass
		else:
			try:
				data[_] = [i for i in dir(_) if not i.startswith('_')]
			except:
				print('somebad shit accured')
	return data

def jsondump(filename: str, content: dict):
	if path.exists(filename):
		with open(filename, "w+") as file:
			file.write(dumps(content, indent=4))
			return 0
	return 1

def getInfo(classname):
	return loads(open("request.json").read())[classname]

content = getDIR(request)

# jsondump("request.json", content)
print(getInfo('Request'))