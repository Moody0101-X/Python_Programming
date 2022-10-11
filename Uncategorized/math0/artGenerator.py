from variables import randomcolors, lightBlue, randomcolors
from Graph import graphPoints
a = 100
x = [i for i in range(1000)]
y = [i/2*+200+100  for i in x]
graphPoints(x, y, color=randomcolors, radius=1, move=True)