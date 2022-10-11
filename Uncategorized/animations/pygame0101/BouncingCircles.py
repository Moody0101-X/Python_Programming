"""
this module helps you draw cirlces in pygame.
"""


import pygame
from random import choice, randint, uniform
from BoilerPlate import code
from math import sin, tan, cos
from numpy import random
class circle:
    
    def __init__(self, win, x, y, color, radius):
        self.win = win
        self.x = x
        self.y = y
        self.color = color
        self.radius = radius
        self.velocity = 4
        self.Ydirection = 1
        self.Xdirection = 1
    def setdimensions(self, w, h):
        self.w = w
        self.h = h

    def draw(self):
        if self.x < self.w - 50 and self.y < self.h - 50:
            pygame.draw.circle(self.win, self.color, (self.x, self.y), self.radius)
            
    def move(self):
        
        if self.x >= self.w - 50:
            self.Xdirection = -1
        elif self.x <= 50:
            self.Xdirection = 1
        if self.y >= self.h - 50:
            self.Ydirection = -1
        elif self.y <= 50:
            self.Ydirection = 1
        
        self.y += self.Ydirection * randint(0, 5) * uniform(0, 1) * randint(0, 5)
        self.x +=  self.Xdirection * randint(0, 5) * uniform(0, 1) * randint(0, 5)
    def move_(self):
        
        if self.x >= self.w - 50:
            self.Xdirection = -1
        elif self.x <= 50:
            self.Xdirection = 1
        if self.y >= self.h - 50:
            self.Ydirection = -1
        elif self.y <= 50:
            self.Ydirection = 1
        
        self.y += self.Ydirection * cos(randint(0, 360)) * randint(0, 5)
        self.x +=  self.Xdirection * cos(randint(0, 360)) * randint(0, 5)

class BouncingCircles(code):
    """ A cirlce wrapper class that inherits from code. """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.circles = [
            circle(self.window, x, y, (randint(0, 255), 0, 0, 50), 1) for x, y in zip([ i for i in range(50, 1000)], [ i for i in range(50, 1000)])
        ]
            
        self.run_()
    def draw(self) -> None:
        for i in self.circles:
            i.draw()

    def run_(self) -> None:
        self.clock.tick(30)
        while self.run:
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    self.run = False
            pygame.display.update()
            # self.window.fill(self.Backgroundcolor)
            for i in self.circles:
                i.setdimensions(self.width, self.height)
                func = choice([i.move, i.move_])
                func()
            self.draw()
        pygame.quit()

if __name__ == "__main__":
    BouncingCircles("Circle", (0, 0, 0), 1080, 720)






"""

RGBA ALPHA
(R, G, B) => RED, GREEN, BLUE
(0, 0, 0) => black
(255, 255, 255) => white
(0, 0, 100) => purple
(?, 0, ?) => pink

"""