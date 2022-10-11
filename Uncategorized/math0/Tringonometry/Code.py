# data = zip(xdata, ydata)
# circles = [Circle(x+10, y,(19, 19, 19), 3) for x,y in data]

# run = True
# while run:
#     Clock.tick(60)
#     for event in pygame.event.get():
#         if event.type == pygame.QUIT:
#             run = False
#     for circle in circles:
#     	circle.draw()
#     	# circle.Move()
#     pygame.display.update()
#     # window.fill((255,255,255))
# pygame.quit()




"""
I was trying to make a graph using Matplotlib but I Had some problems in importing the lib
so it is better to make a new mechanism to plot using numpy, pygame.

class Grapher(p):
	def __init__(self, x_axis: list, y_axis: list, x_label:str="x", y_label: str="y") -> None:
		
		self.x_axis = x_axis
		self.y_axis = y_axis
		self.y_label = y_label
		self.x_label = x_label
		p.ylabel(self.y_label)
		p.xlabel(self.x_label)
		if self.y_axis and self.x_label:
			p.plot(self.x_axis, self.y_label)
		else:
			p.plot(self.x_axis)
		p.show()

def main(x=None, y=None):
	if x and y:
		Grapher(x, y)
	else:
		Grapher(x)


if __name__ == '__main__':
	main([i for i in range(100)], [i for i in range(100)])
import pandas as pd
data = pd.read_csv("C:\\Users\\pc\\Documents\\survey_results_public.csv")
print(data.info())
"""