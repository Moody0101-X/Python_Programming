import re
from tkinter import LabelFrame, Label, Button, Tk, Entry
import OppDownloader
from tkinter.ttk import Progressbar
from threading import Thread
from pafy import new

try:
    from colors import *
except:
    try:
        from colorama import Fore
        BLACK = Fore.BLACK
        BLUE = Fore.BLUE
        CYAN = Fore.CYAN
        GREEN = Fore.GREEN
        LIGHTBLACK_EX = Fore.LIGHTBLACK_EX
        LIGHTBLUE_EX = Fore.LIGHTBLUE_EX
        LIGHTCYAN_EX = Fore.LIGHTCYAN_EX
        LIGHTGREEN_EX = Fore.LIGHTGREEN_EX
        LIGHTMAGENTA_EX = Fore.LIGHTMAGENTA_EX
        LIGHTRED_EX = Fore.LIGHTRED_EX
        LIGHTWHITE_EX = Fore.LIGHTWHITE_EX
        LIGHTYELLOW_EX = Fore.LIGHTYELLOW_EX
        MAGENTA = Fore.MAGENTA
        RED = Fore.RED
        RESET = Fore.RESET
        WHITE = Fore.WHITE
        YELLOW = Fore.YELLOW

    except:
        print("install colorama Using pip install colorama!")

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# Author : Moody0101
# version: 1
# github: github.com/Moody0101
# the Youtube downloader section
# just be cause I could not import it, so it made me mad, so it is better to copy it and paste
# dependencies are  { pafy, youtube-dl, os, Tkinter}
# to know how to use the YoutubeDownloader class you can go to this repo
# github.com/Moody0101/youtubeDownloader
# https://www.youtube.com/watch?v=smqhSl0u_sI
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# GUI progress bar

"""

It did not work, so I had to make a terminal based one for the callback

"""
class _ProgressBar(Tk):

    def __init__(self, t):
        super().__init__()
        self.t = t
        self.wm_minsize(500, 150)
        self.title = "Progress :)"
        self.configure(background="white")
        self.wm_resizable(False, False)
        self.frame = LabelFrame(self, text=None, background="black", bd=0, padx=200, pady=50)
        self.frame.pack()
        self.UI()
        self.run()
    def UI(self):
        self.p = Progressbar(self.frame, value=0, orient='horizontal', mode='determinate', length=280)
        self.p.grid(column=0, row=0, columnspan=2, pady=5, padx=3)
        self.label = Label(self.frame, text=f"0%")
        self.label.grid(column=3, row=0, pady=5, padx=3)
        self.titleL = Label(self.frame, text=self.t) 
        self.titleL.grid(column=0, row=2,columnspan=2, pady=5, padx=3)
        self.update_idletasks()
        
    def Prog(self,total, recvd, ratio, rate, eta):
        while total < recvd:
            self.p["value"] = (total/100)*recvd
            self.label["text"] = f"{(total/100)*recvd}%"
            self.update_idletasks()
        print(total)
    def run(self):
        self.mainloop()

class ConsoleProgress:
    def __call__(self ,total, recvd, *args):
        # print(f"{CYAN}=======================================================================================")
        print(f"{YELLOW} TOTAL SIZE: {total/10**6} MB {CYAN} => Received : {recvd/10**6} MB  ", end="\r")
        # print(f"{CYAN}================================================================================================{RESET}")

class YDL(Tk):
    searchButton: Button

    def __init__(self):
        super().__init__()
        self.iconbitmap('./Logo.ico')
        self.wm_iconify()
        self.title = "YTDL :)"
        self.configure(background="white")
        self.wm_resizable(False, False)
        self.wm_minsize(500, 100)
        self.frame = LabelFrame(self, text=None, background="whitesmoke", bd=0,padx=200, pady=50)
        self.frame.pack(padx=10, pady=10)
        self.main()
        

    def searchQuery(self):
        self.video = OppDownloader.youtubeDownloader(str(self.ENTRY.get()))
        self.stats = self.video.displayStats
        self.titleLab = Label(
            self,
            text=self.stats['title'],
            background="white",
            fg="black",
            font="sans-serif",
            bd=0,
            padx=70,
            pady=30)
        self.titleLab.pack(pady=10)
        self.authorLab = Label(
            self,
            text=self.stats['author'],
            background="white",
            fg="black",
            font="sans-serif",
            bd=0,
            padx=70,
            pady=30)
        self.authorLab.pack(pady=10)
        self.durationLab = Label(
            self,
            text=self.stats['duration'],
            background="white",
            fg="black",
            font="sans-serif",
            bd=0,
            padx=70,
            pady=30)
        self.durationLab.pack(pady=10)
        self.download = Button(
            self, text="download", bd=0, background="black", font="sans-serif, 10",
            padx=10, pady=5, fg="white", command=self.downloadUi
        )
        self.download.pack(pady=10)
    # def CallBack(self, total, recvd, ratio, rate, eta):
    #     BAR = _ProgressBar(self.t)
    #     BAR.run()
    #     BAR.progress(total, recvd)

    def destroyEverything(self):
        self.ENTRY.destroy()
        self.durationLab.destroy()
        self.authorLab.destroy()
        self.titleLab.destroy()
        self.download.destroy()
        self.searchButton.destroy()

    def downloaditem(self, x):
        try:
            x.download(quiet=True,callback=ConsoleProgress())
        except KeyboardInterrupt:
            print(f"{RED}\n")
            print(f" Key interruption :( EXITING {RESET}")  
    def downloadUi(self):
        self.destroyEverything()
        self.frame.configure(padx=0, background="white")
        self.configure(background="white")
        dps = [i for i in self.video.displayStats['allStreams']]
        j = 0
        k = 0
        for i in dps:
            repr = str(i).split('@')[1] 
            
            self.i = Button(
                self.frame, text=repr + ' '  +str(int(i.get_filesize() * 10 ** -6))+'MB', padx=50, pady=10, bd=0,
                fg="White", font="sans-serif", background="black",
                command=lambda: self.downloaditem(i)
            )
            self.i.grid(pady=10, padx=10, row=k, column=j)
            j += 1
            if j >= 3:
                k += 1
                j = 0

    def main(self):
        self.ENTRY = Entry(self.frame, border=0, font="sans-serif")
        self.ENTRY.grid(column=0, row=0, columnspan=3, ipadx=100, ipady=5, padx=10)
        self.searchButton = Button(self.frame, text="Search Now",
                                   bd=0, bg="white", fg="black", font="sans-serif",
                                   command=self.searchQuery)
        self.searchButton.grid(column=4, row=0)
    
root = YDL()
root.mainloop()
