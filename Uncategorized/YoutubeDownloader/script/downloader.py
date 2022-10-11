from pafy import new
import requests
import os
"""
My first python script that got me impressed about how powerful python can be
"""
def autodownload():
    if choice1 == "video":
        url = input("enter the link of your video : ")
        video = new(url)
        stream = video.getbestvideo()
        print("author : " + video.author + "\n" + "title : " + video.title + "\n" + "duration : " + video.duration)
        print(stream)
        print("loading......")
        stream.download()
    elif choice1 == "audio":
        directory = os.path.join('c:\\users\\pc\\Documents\\Music', "temp", "python")
        if not os.path.exists(directory):
            os.mkdir(directory)
        os.chdir('c:\\users\\pc\\Documents\\Music')
        cwd = os.getcwd()
        print(cwd)
        audio_url = input("enter the link of your song : ")
        audio = new(audio_url)
        stream = audio.getbestaudio()
        print(stream)
        print("author : " + audio.author + "\n" + "title : " + audio.title + "\n" + "duration : " + audio.duration)
        print("loading......")
        stream.download()


def video_file():
    directory = os.path.join('c:\\users\\pc\\Documents\\videos')
    if not os.path.exists(directory):
        os.mkdir(directory)
    os.chdir('c:\\users\\pc\\Documents\\videos')
    cwd = os.getcwd()
    print(cwd)


def change_dir():
    os.chdir('c:\\users\\pc\\Documents\\videos')
    cwd = os.getcwd()
    print(cwd)


def courses_file():
    dir = os.path.join('c:\\users\\pc\\desktop\\courses')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)
    cwd = os.getcwd()
    print(cwd)


def filesdownload():
    dir = os.path.join('c:\\users\\pc\\desktop\\files')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)
    cwd = os.getcwd()
    print(cwd)
    url = input("enter the link of the file you want to download : ")
    Req = requests.get(url, allow_redirects=True)
    file_name = input("enter the file name : ")
    print("loading.....")
    open(file_name, 'wb').write(Req.content)
    print("finished downloading the file !!")


choice1 = input("What do want to download Anime/latestalbums/template/audio/video/file : ")

if choice1 == "video":
    type = input("is it a course : ")
    if type == "yes":
        courses_file()
    elif type == "no":
        video_file()
if choice1 == "video" or choice1 == "audio":
    choice = input("method choice or autodownload : ")
    if choice == "autodownload":
        autodownload()
        print("download complete!! Enjoy ur file")
    elif choice == "choice":
        if choice1 == "video":
            url = input("enter the link of your video : ")
            video = new(url)
            print("author : " + video.author + "\n" + "title : " + video.title + "\n" + "duration : " + video.duration)
            stream = video.streams
            for i in stream:
                print(str(stream.index(i)) + " ----> " + str(i))
            index = int(input("enter the quality you want : "))
            Quality = stream[int(index)]
            print("you have chosen this quality " + str(stream[int(index)]) + "....")
            Quality.download()
        elif choice1 == "audio":
            dir = os.path.join('c:\\users\\pc\\Documents\\Music')
            if not os.path.exists(dir):
                os.mkdir(dir)
            os.chdir('c:\\users\\pc\\Documents\\Music')
            cwd = os.getcwd()
            print(cwd)
            audio_url = input("enter the link of your song : ")
            video = new(audio_url)
            print(video.author + "\n" + video.title + "\n" + video.duration)
            stream = video.audiostreams
            for i in stream:
                print(str(stream.index(i)) + " ----> " + str(i))
            index = int(input("enter the quality you want : "))
            Quality = stream[int(index)]
            print("you have chosen this quality " + str(stream[int(index)]) + "....")
            Quality.download()
elif choice1 == "template" or choice1 == "Template":
    dir = os.path.join('c:\\users\\pc\\Documents\\Templates', 'temp', 'python')
    if not os.path.lexists(dir):
        os.mkdir(dir)

    type = input("which type of template do you want {classic/modern} : ")
    if type == "classic":
        classic = "https://binaries.templates.cdn.office.net/support/templates/fr-fr/tf16402488_win32.dotx"
        req1 = requests.get(classic, allow_redirects=True)
        filename1 = "classic.dotx"
        open(filename1, 'wb').write(req1.content)
    elif type == "modern":
        modern = "https://binaries.templates.cdn.office.net/support/templates/fr-fr/tf16392716_win32.dotx"
        req = requests.get(modern, allow_redirects=True)
        filename = "modern.dotx"
        open(filename, 'wb').write(req.content)
elif choice1 == "file":
    filesdownload()
print("download complete!!!! enjoy")
print("thanks for using my script\n" + "Scripted by : MOODY\n Email : kinumoody@gmail.com")

# I am thinking about gui this though
