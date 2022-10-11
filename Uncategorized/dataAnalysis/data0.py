"""
How to plot functions using matplotlib, and math.
"""

from matplotlib import pyplot
from math import sin, radians, tan, cos



class myFuncs:
	def sinAn(x: int | str) -> int:
		try:
			return (sin(x) + cos(x) + tan(x))/x**-19
		except:
			return (sin(x) + cos(x) + tan(x))/(x-1)**-2


	def f(x):
		try:
			return tan(sin(sin(sin(sin(sin(sin(sin(sin(1/x*10)))))))))
		except:
			return tan(sin(sin(sin(sin(sin(sin(sin(sin(1+(x+10))))))))))


	def division(x):
		try:
			return 1/tan(x)
		except:
			return 1/(tan(x) + 0.1)

	def RaisedToTwo(x):
		return x**2


class plot:
	def __init__(self, x_axis, func: callable):
		self.x_axis = x_axis
		self.func = func
		pyplot.plot(self.x_axis, [func(y) for y in self.x_axis])
		pyplot.show()


plot([i*90 for i in range(10)], tan)
