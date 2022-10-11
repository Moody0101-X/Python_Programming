import pygame
from math import sin, cos, tan, e, prod, atanh, sqrt
from itertools import chain
from random import choice

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


class Graph:
	clock = pygame.time.Clock()
	pygame.init()
	pygame.display.set_caption("GRAPH")

	def __init__(self, height, width, Backgroundcolor: tuple):
		self.height = height
		self.width = width
		self.Backgroundcolor = Backgroundcolor
		self.window = pygame.display.set_mode((self.height, self.width))
		self.window.fill(self.Backgroundcolor)
		self.run = True
	def draw(self):
		
		pygame.display.update()
	def run_(self, a, b, c):
		self.clock.tick(30)
		while self.run:
			for event in pygame.event.get():
				if event.type == pygame.QUIT:
					self.run = False
			pygame.display.update()
			keys = pygame.key.get_pressed()
			if keys[pygame.K_DOWN]:
				c -= .001
				
				self.window.fill(self.Backgroundcolor)
			if keys[pygame.K_UP]:
				c += .001
				
				self.window.fill(self.Backgroundcolor)
			if keys[pygame.K_LEFT]:
				b += .001
				self.window.fill(self.Backgroundcolor)
			if keys[pygame.K_RIGHT]:
				b -= .001
				self.window.fill(self.Backgroundcolor)
			
			self.PlotF(a, b, c)
			self.draw()
		pygame.quit()
	@property
	def origin(self):
		return {
		'x': self.width/2,
		'y': self.height/2,
		}

	def PlotF(self, Func, Percision, ZOOM=1, Start=None, End=None):
		ZOOM *= 20
		self.O = self.origin
		if not (Start and End):
			Start = -10000
			End = 10000
		if not Start:
			Start = -10000
		if not End:
			End = 10000
		for i in [i*Percision for i in range(Start, End)]:
			try:
				pygame.draw.circle(self.window, (0, 0, 100), ((self.O['x'] + i*ZOOM), (self.O['y'] + (Func(i)*-1)*ZOOM)), 5)
			except Exception as e:
				pass
	def plot(self, x_axis: list, y_axis: list, ZOOM=1):
		data = zip(x_axis, y_axis)
		ZOOM *= 30
		self.O = self.origin
		for x, y in data:
			pygame.draw.circle(self.window, (0, 0, 0), ((self.O['x'] + x *ZOOM), (self.O['y'] + y*ZOOM)), 5*ZOOM)
def Function(x):
	return x**2

def division(x):
	return 1/tan(x)
def sinAn(x):
	return (sin(x) + cos(x) + tan(x))/x**-2
def Euler(x):
	return x**e

def f(x):
	return tan(sin(sin(sin(sin(sin(sin(sin(sin(1/x*10)))))))))


graph = Graph(700, 700, (255, 255, 255))

graph.plot([i for i in range(10)], [i*.10 for i in range(10)])
graph.run_(sqrt, .00001,1)