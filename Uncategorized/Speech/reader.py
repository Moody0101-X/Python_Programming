"""
A software to read files outloud.

"""

from tkinter import Tk, Button, filedialog, Label, LabelFrame
import pyttsx3 
import PyPDF2
from gtts import gTTS
from time import  sleep

class reader(Tk):

	def __init__(self):
		super().__init__()
		# self.iconbitmap('./Logo.ico')
		self.readingEngine = pyttsx3.init()
		self.file = None
		self.wm_resizable(False, False)
		self.wm_minsize(200, 50)
		self.title('file Reader:)')
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
			self.readButton = Button(self.frame, text=f'read the file', bd=0, bg="white", fg="black", font="Myriad-Pro", command=self.read)
			self.readButton.grid(column=0, row=0, padx=10, pady=10)
			self.components.append(self.readButton)
			self.saveButton = Button(self.frame, text=f'save {name}.mp3', bd=0, bg="white", fg="black", font="Myriad-Pro", command=self.save)
			self.saveButton.grid(column=1, row=0, padx=10, pady=10)
			self.components.append(self.saveButton)
	def select(self):
		self.file = filedialog.askopenfilename(initialdir='.', title='select a file', filetypes=(('text files', '*.txt'), ('all files', '*.*')))
		self.fileName = self.file.split('/')[-1].split('.')[0]
		self.extention = self.file.split('/')[-1].split('.')[1]
		self.UI(name=self.fileName, n=100)
	def save(self):
		if self.extention == 'txt':
			self.readingEngine.save_to_file(self.file, f"{self.file}.mp3")
			self.readingEngine.runAndWait()
			self.label = Label(self, text='Saved!')
			self.label.pack(pady=10, padx=10)
		else:
			self.savePDF()
			self.label = Label(self, text='Saved!')
			self.label.pack(pady=10, padx=10)
	def read(self):
		if self.extention == 'txt':
			self.say()
		else:
			
			self.readPDF()
			self.readingEngine.runAndWait()
	def savePDF(self):
		language = 'en'
		myAudio = gTTS(text=self.getText(), lang=language, slow=False)
		myAudio.save(f"{self.fileName}.mp3")
	def readPDF(self):
		self.say(self.getText())
	def say(self, TEXT):
		self.readingEngine.say(TEXT)
		self.readingEngine.runAndWait()
	def getText(self):
		pdf_Reader = PyPDF2.PdfFileReader(open(self.file, 'rb'))
		pages = pdf_Reader.numPages
		return " ".join([pdf_Reader.getPage(i).extractText() for i in range(pages)])
	
instance = reader()
instance.mainloop()