import pyttsx3
from pynput.keyboard import Listener

def Encourage_me():
	Engine = pyttsx3.init()
	Engine.say("""
	Hey, I know it is not the right time but I believe that you are a great person, you 
	can do a lot of good things, just believe in yourself as I believe in you, try your best
	take your time be happy and comfortable.
	""")
	Engine.runAndWait()

def process(key):
	try:
		if key == key.alt_l:
			return True
		else:
			return False
	except:
		return False

def on_press(key):
	if process(key):
		Encourage_me()
	else:
		pass

with Listener(on_press=on_press) as l:
    l.join()
    print(Fore.RESET)