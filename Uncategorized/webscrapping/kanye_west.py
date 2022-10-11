import requests
from bs4 import BeautifulSoup
from os import path, mkdir, chdir
from sys import exit
from tkinter import Tk, Button, Label
from time import sleep

"""
still  working on it..
"""
def getContent(url: str):
    """
    getting the html..
    """
    req = requests.get(url)
    return req.content

class kanye(Tk):
    """
    ui for downloading :) 
    """
    def __init__(self, *args):
        super().__init__()
        self.wm_resizable(False, False)
        self.wm_minsize(500, 300)
        self.title('Kayne west discography :)')
        self.configure(background="white")
        self.main()
    def main(self):
        """
        buttons and stuff..
        """
        self.lb = Label(self, text="Click the download button to download every kanye west album", fg="black", font="sans-serif", padx=10, pady=10)
        self.lb.pack(ipadx=5, pady=100)
        self.down = Button(self, text="Download",bd=0, border=0, padx=10, pady=10, fg="white", bg="black", command=self.hide)
        self.down.pack(padx=5, pady=5) 
        # return [self.lb, self.down]
    def hide(self):
        self.lb.destroy()
        self.down.destroy()
        self.text2 = Label(self, text="Please wait it would take a while cuz it is is a big chunk of files, you will find them in the download folder", fg="white",bg="black", font="sans-serif", padx=10, pady=10) 
        self.text2.pack(ipadx=5, ipady=200)
        self.download()
    def download(self):
        """
        the func that will download everythin.. from a blogspot in the web.
        """
        if not path.exists('./download'):
            mkdir('./download')
            chdir('./download')
        else:
            chdir('./download') 
        try:
            url0 = "http://productoilicito2.blogspot.com/2017/06/kanye-west-discografia-mediafire-2001.html"
            for _ in BeautifulSoup(getContent(url0), 'html.parser').findAll('a'):
                if str(_.get("href")).startswith('http://www.me'):
                    name = 1
                    downloadpage = getContent(str(_.get("href")))
                    file0 = BeautifulSoup(downloadpage, 'html.parser').find(id="downloadButton")
                    file1 = getContent(file0.get("href"))
                    with open(f'{name}.7z', 'wb+') as f:
                        f.write(file1)
                    name += 1
            int("done")
            exit()
        except
            print('network failed, make sure you have network connection')
            sleep(2)
            exit()
window = kanye()
window.mainloop()

