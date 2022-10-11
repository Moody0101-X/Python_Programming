

import pygame
from random import choice, randint
from BoilerPlate import code
from math import sin
from numpy import random 





class Circle(code):
    """ A cirlce wrapper class that inherits from code. """
    def __init__(self, x, y, color, radius,MovinFunction=None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.x = x
        self.y = y
        self.color = color
        self.radius = radius
        self.MovinFunction = MovinFunction
        self.velocity = 3
        self.Xdirection, self.Ydirection = 1, 1

        self.run_()
    
    def draw(self) -> None:
        try:
            pygame.draw.circle(self.window, self.color, (self.x, self.y), self.radius)
        except:
            pygame.draw.circle(self.window, (0, 0, 0), (self.x, self.y), self.radius)
    
    def move(self):
        if not self.MovinFunction:
            
            if self.x > self.width - 10:
                self.Xdirection *= -1
            
            if self.x < 5:
                self.Xdirection *= -1
            
            if self.y > self.height - 10:
                self.Ydirection *= -1


            if self.y < 5:
                self.Ydirection *= -1

            self.x += (self.velocity*self.Xdirection)*randint(0, 3)
            self.y += (self.velocity*self.Ydirection)*randint(0, 3)

        else:
            self.x, self.y = self.MovinFunction(self.x, self.y)
    def addObject(self) -> None:
        pass
    def run_(self) -> None:
        self.clock.tick(30)
        while self.run:
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    self.run = False
            self.move()
            pygame.display.update()
            self.window.fill(self.Backgroundcolor)
            self.draw()
        pygame.quit()


if __name__ == "__main__":
    Circle(randint(0, 1200), randint(0, 700), (0, 0, 0), 20 ,Title="Circle", Backgroundcolor=(255, 255, 255), width=1200, height=700)