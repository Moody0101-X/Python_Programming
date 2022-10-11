

class Player1:
	
	def __init__(self, name, surname, Xp):
		self.name = name
		self.surname = surname
		self.Xp = Xp
	
	def getName(self):
		print(self.name)


class plyer2(Player1):
	def __init__(self, blood, *args, **kwargs) -> None:
		super().__init__(*args, **kwargs)
		self.blood = blood
	def getBlood(self):
		print(self.blood)
	
c = plyer2(109091890, 1, 1, 2)
c.getBlood()
c.getName()