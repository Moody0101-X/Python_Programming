
import numpy as n
import pygame
from math import sin, pi, radians, cos, tan
from random import randint, choice

class MissingChoordinates(Exception):
	pass


class Circle(object):
	
	def __init__(self,window, x: int, y: int, color: tuple, radius: int):
		self.window = window
		self.x = x
		self.y = y
		self.color = color
		self.radius = radius
		self.vel = 50
		self.dx = 1
		self.dy = 1
		self.velocity = 3
	def spawn(self, window):
		try:
			pygame.draw.circle(self.window, self.color, (self.x, self.y), self.radius)
		except:
			pygame.draw.circle(self.window, (0, 0, 0), (self.x, self.y), self.radius)

	def draw(self):
		self.spawn(self.window)
		
	def Move(self):
		if self.x > 1000:
			self.dx = -1
		if self.x < 10:
			self.dx = 1
		
		self.x += self.velocity*self.dx*randint(1, 3)
		
		if self.y > 700:
			self.dy = -1
		if self.y < 10:
			self.dy = 1
		# if self.y == 200:
		# 	self.dy = -1
		self.y += self.velocity*self.dy*randint(1, 3)
		# print(self.y, self.dy)
		# print(self.x, self.dx)
class graph:
	pygame.init()
	window = pygame.display.set_mode((1000, 700))
	window.fill((255, 255, 255))
	pygame.display.set_caption('circles')
	Clock = pygame.time.Clock()
	def __init__(self, x_axis: list, y_axis: list, radius, color, move=True, paint=False) -> None:
		# super(pygame).__init__()
		self.x_axis = x_axis
		self.y_axis = y_axis
		self.radius = radius
		self.move = move
		self.color = color
		self.paint = paint
		self.data = zip(self.x_axis, self.y_axis)
		if self.color:
			if isinstance(self.color, list):
				self.Points = [Circle(self.window, x+10, y,choice(self.color), self.radius) for x,y in self.data]
			else:
				self.Points = [Circle(self.window, x+10, y,self.color, self.radius) for x,y in self.data]
		else:
			self.Points = [Circle(self.window, x+10, y,(19, 19, 19), self.radius) for x,y in self.data]

		self.init = True
	def run(self):
		while self.init:
			self.Clock.tick(60)
			for event in pygame.event.get():
				if event.type == pygame.QUIT:
					self.init = False
			for point in self.Points:
					point.draw()
					if self.move:
						point.Move()

			pygame.display.update()
			if self.paint:
				self.window.fill((255, 255, 255))
			else:
				pass
		pygame.quit()

def graphTrigPoints(angels=[i*10 for i in range(200)],
	trig=sin,
	pointcount=100,
	radius=10,
	color=(19, 19, 19)
	):
	"""
	angels:
	"""
	xdata = [i/3 for i in angels]
	ydata = [trig(radians(i))*30+500 for i in angels]
	graph(xdata, ydata, radius, color).run()

def graphPoints(x_axis, y_axis, radius=2,color=(19, 19, 19), move=False, paint=False):
	if len(x_axis) != len(y_axis):
		raise MissingChoordinates("x_axis and y_axis have diffrent lengths, so it was not possible to associate every point with a (x, y) choord")
	else:
		graph(x_axis, y_axis, radius, color, move, paint).run()