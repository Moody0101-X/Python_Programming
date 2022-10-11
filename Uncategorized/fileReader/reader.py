"""

GITHUBHOME = https://github.com
a software to read files.
author: Moody0101
github: GITHUBHOME/Moody0101

"""


from tkinter import Tk, Button, filedialog, Label, LabelFrame, Entry
import pyttsx3
import PyPDF2
from gtts import gTTS
from time import  sleep
from multiprocessing import Process
class reader(Tk):
	def __init__(self):
		super().__init__()
		try:
			self.iconbitmap('./POPI.ico')
		except:
			pass
		self.readingEngine = pyttsx3.init()
		self.file = None
		self.wm_resizable(False, False)
		self.wm_minsize(200, 50)
		self.title('file Reader')
		self.configure(background="white")
		self.frame = LabelFrame(self, text=None, background="#191919", bd=0, padx=200, pady=50)
		self.frame.pack()
		self.components = []
		self.UI()
	def UI(self, name:str='select a file', n: int=0):
		if n == 0:
			self.firstButton = Button(self.frame, text=name, bd=0, bg="white", fg="black", font="Myriad-Pro", command=self.select)
			self.firstButton.grid(column=0, row=0)
		else:
			self.firstButton.destroy()
			self.l0 = Label(self.frame, text="Type number of pages to read/save if it is a pdf", bg="#191919", fg="white", font="Myriad-Pro")
			self.l0.grid(column=0, row=0, columnspan=2, padx=10, pady=10)
			self.l1 = Label(self.frame, text="Default=allpages", bg="#191919", fg="white", font="Myriad-Pro")
			self.l1.grid(column=0, row=1, columnspan=2, padx=10, pady=10)
			self.ent = Entry(self.frame, border=0, font="sans-serif")
			self.ent.grid(column=0, row=2, columnspan=2, padx=10, pady=10, ipadx=10)
			self.readButton = Button(self.frame, text=f'read the file', bd=0, bg="white", fg="black", font="Myriad-Pro", command=self.read)
			self.readButton.grid(column=0, row=3, padx=10, pady=10)
			self.components.append(self.readButton)
			self.saveButton = Button(self.frame, text=f'save {name}.mp3', bd=0, bg="white", fg="black", font="Myriad-Pro", command=self.save)
			self.saveButton.grid(column=1, row=3, padx=10, pady=10)
			self.components.append(self.saveButton)
	def select(self):
		self.file = filedialog.askopenfilename(initialdir='.', title='select a file', filetypes=(('text files and pdf files', '*.txt *.pdf'), ('all files', '*.*')))
		self.fileName = self.file.split('/')[-1].split('.')[0]
		self.extention = self.file.split('/')[-1].split('.')[1]
		self.UI(name=self.fileName, n=100)
	def save(self):
		try:
			pages = int(self.ent.get())
		except:
			pages = ""
		print(pages)
		if self.extention == 'txt':
			self.label = Label(self, text='saving, please wait..')
			self.label.pack(pady=10, padx=10)
			self.saveTXT()
			self.label = Label(self, text='Saved!')
			self.label.pack(pady=10, padx=10)
		else:
			self.label = Label(self, text='saving, please wait..')
			self.label.pack(pady=10, padx=10)
			self.savePDF(pages)
			self.label = Label(self, text='Saved!')
			self.label.pack(pady=10, padx=10)
	def read(self):
		try:
			pages = int(self.ent.get())
		except:
			pages = ""
		print(pages)
		if self.extention == 'txt':
			self.label = Label(self, text='Reading..')
			self.label.pack(pady=10, padx=10)
			self.say(open(self.file, 'r').read())
		else:
			self.label = Label(self, text='Reading..')
			self.label.pack(pady=10, padx=10)
			self.readPDF(pages)
			self.readingEngine.runAndWait()
	def savePDF(self, pages: str):
		language = 'en'
		if pages == "":
			myAudio = gTTS(text=self.getText(), lang=language, slow=False)
			myAudio.save(f"{self.fileName}.mp3")
		else:
			myAudio = gTTS(text=self.getText(pages), lang=language, slow=False)
			myAudio.save(f"{self.fileName}.mp3")
	def readPDF(self, numpage=None):
		if numpage == None:
			self.say(self.getText())
	def say(self, TEXT):
		self.readingEngine.say(TEXT)
		self.readingEngine.runAndWait()
	def getText(self, pagenum=None):
		pdf_Reader = PyPDF2.PdfFileReader(open(self.file, 'rb'))
		if pagenum == None:
			pages = pdf_Reader.numPages
		else:
			pages = pagenum
		return " ".join([pdf_Reader.getPage(i).extractText() for i in range(pages)])
	def saveTXT(self):
		self.readingEngine.save_to_file(self.file, f"{self.fileName}.mp3")
		self.readingEngine.runAndWait()
instance = reader()
instance.mainloop()