"""
Animation using the sin values and the module I made to help me using
pygame to plot points in the x,y axiss
"""



from math import sin
from Graph import graphTrigPoints
from random import choice
from variables import white

whitesmoke = (200, 200, 200)
black = (19, 19, 19)
BLUE = (0, 0, 100)

graphTrigPoints(trig=sin, color=white)