from pafy import new
from tkinter import *
import os
"""
I enjoyed doing this thing.. this is m first gui ever, however I did not finish this one
instead I have created another one with the same functionality.
"""
root = Tk()
root.iconbitmap("./icon.wmf")
root.configure(background='#161624')
root.title('Youtube-UI')
root.tk_focusFollowsMouse()
LINK = Entry(root, width=50, border=0, highlightthickness=1, highlightcolor='#161610', takefocus=True
             , font='sans-serif', justify=LEFT, relief='solid')
LINK.pack(padx=20, pady=20, ipady=6)
myframe = LabelFrame(root, border=0, text=None, padx=5, pady=5, bg='white')
myframe.pack(padx=5, pady=5)


def download_video():
    global i, j
    root.destroy()
    root2 = Tk()
    Iframe2 = LabelFrame(root2, border=0, text=None, padx=5, pady=5, bg='white')
    Iframe2.pack(padx=5, pady=5)
    current = str(os.getcwd())
    path = os.path.join(str(current) + '\\' + 'video')
    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)
    print(path)
    for item in stream:
        global i, j
        i = 0
        j = 0
        while i < len(stream):
            btn = Button(Iframe2, repeatdelay=500,
                         text="{}MB".format(str(item.get_filesize() * 10 ** -6)) + " " + str(item), padx=10,
                         pady=10,
                         fg='#ecf0f1',
                         bg='#f1c40f', font='sans-serif', border=0, highlightcolor='blue')
            btn.grid(column=0 + i, row=0 + j, padx=5)
            i += 1
            if i % 4 == 0:
                j += 1
    root2.mainloop()


def download_audio():
    global i, j
    i = 0
    j = 0
    root.destroy()
    root2 = Tk()
    Iframe2 = LabelFrame(root2, border=0, text=None, padx=5, pady=5, bg='white')
    Iframe2.pack(padx=5, pady=5)
    current = str(os.getcwd())
    path = os.path.join(str(current) + '\\' + 'Music')
    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)
    print(path)
    for item in video2:
        btn = Button(Iframe2, repeatdelay=500,
                     text="{0}MB".format(str(item.get_filesize() * 10 ** -6)) + " " + str(item), padx=10, pady=10,
                     fg='#ecf0f1',
                     bg='#f1c40f', font='sans-serif', border=0, highlightcolor='blue')
        btn.grid(column=0 + i, row=0 + j, padx=5)
        i += 1
        if i % 4 == 0:
            j += 1
    root2.mainloop()


def downloadvid():
    global stream, video

    url1 = str(LINK.get())
    video = new(url1)
    stream = video.getbestvideo
    author = Label(myframe, text="author : " + video.author, bg='yellow', width=40)
    author.grid(column=0, row=1, columnspan=2, padx=5, pady=5)
    title = Label(myframe, text="title : " + video.title)
    title.grid(column=0, row=2, columnspan=2, padx=5, pady=5)
    duration = Label(myframe, text="duration : " + video.duration)
    duration.grid(column=0, row=3, columnspan=2, padx=5, pady=5)
    b = Button(myframe, repeatdelay=500, text='Are you sure?', command=download_video, fg='#ecf0f1',
               bg='#f1c40f', font='sans-serif', border=0, highlightcolor='blue').grid(column=0, row=4, columnspan=2,
                                                                                      padx=5, pady=5)


def downloadaud():
    global video2, stream2
    url2 = str(LINK.get())
    video2 = new(url2)
    stream2 = video2.getbestaudio
    author = Label(myframe, text="author : " + video2.author, bg='yellow', width=40)
    author.grid(column=0, row=1, columnspan=2, padx=5, pady=5)
    title = Label(myframe, text="title : " + video2.title)
    title.grid(column=0, row=2, columnspan=2, padx=5, pady=5)
    duration = Label(myframe, text="duration : " + video2.duration)
    duration.grid(column=0, row=3, columnspan=2, padx=5, pady=5)
    b = Button(myframe, repeatdelay=500, text='Are you sure?', command=download_audio, fg='#ecf0f1',
               bg='#f1c40f', font='sans-serif', border=0, highlightcolor='blue').grid(column=0, row=4, columnspan=2,
                                                                                      padx=5, pady=5)


buttons = [
    'download_video',
    'download_audio',

]
text = [
    'Download Video',
    'Download Audio'
]

video_button = Button(myframe, text=text[0], padx=10, pady=10, fg='#ecf0f1',
                      bg='#f1c40f', font='sans-serif', border=0, highlightcolor='red', command=downloadvid)
video_button.grid(column=0, row=0, padx=5)

audio_button = Button(myframe, text=text[1], padx=10, pady=10, fg='#ecf0f1',
                      bg='#f1c40f', font='sans-serif', border=0, highlightcolor='blue', command=downloadaud)
audio_button.grid(column=1, row=0, padx=5)
print(root.winfo_width())
root.mainloop()

