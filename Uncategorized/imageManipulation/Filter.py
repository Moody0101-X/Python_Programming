from PIL import Image, ImageFilter
from os import mkdir
filters = [
	i for i in dir(ImageFilter) if not i.startswith('__')
]


def blur(file, out: str) -> bool:
	try:
		file.filter(ImageFilter.BoxBlur(10)).save(out) # Names of the funcs and filters that you can use are in filters list
		# to visualize them u can print the list.
		print("filtered and saved")
	except Exception as e:
		print(e)

def main():
	img = Image.open("girl.png")
	
	blur(img, "./filtered/Color3DLUTgirl2.png")
if __name__ == '__main__':
	print(filters)
	# main()
