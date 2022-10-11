


class dummyClass1:
	print("1")
	dummyClass1()

class dummyClass2(dummyClass1):
	print("2")
	dummyClass1()

x = dummyClass2()
