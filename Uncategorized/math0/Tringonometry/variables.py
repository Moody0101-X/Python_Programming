# rgb

from itertools import chain

white = [

	(i, i, i) for i in range(250, 255)
]
red = [
	(i, 0, 0) for i in range(100, 255)
]

lightBlue =  [
	(0, 10, i) for i in range(240, 255)
]
darkBlue = [
	(0, 10, i) for i in range(0, 100)
]
Green = [
	(0, i, 0) for i in range(240, 255)
]
randomcolors = list(chain([
	lightBlue + red + white	+ darkBlue
]))[0]
