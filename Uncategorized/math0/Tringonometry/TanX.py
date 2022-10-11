"""
Animation using the tan values and the module I made to help me using
pygame to plot points in the x,y axiss
"""


from variables import red

from math import tan
from Graph import graphTrigPoints
from random import choice

whitesmoke = (200, 200, 200)
black = (19, 19, 19)
BLUE = (0, 0, 100)

graphTrigPoints(trig=tan, color=red)