from pafy import new
from time import time, sleep
from pprint import pprint
from os import path, mkdir, chdir


""" 
    introduction : {
        this module is a wrapper that makes you deal with the youtube api, more easily.
    }
    how this module works:
        how youubeDownloader works
            k = youtubeDownloader("url") ==> inits the class.
            k.displayStats() ==> to get the base informations about this video.
            k.pprintStats() ==> to pretty print the stats.
            k.getstreams() => return the list of possible streams you can download.
            k.download..  ==> downloads the video.
            k.dowloandbyindex() ==> prints the streams then you can specify the index you want or the stream 
            you want to download.
        also how downloadData works:
            {still working on it...}
"""


def createdir(arg0: str):
    if not path.exists(arg0):
        mkdir(arg0)
        chdir(arg0)


class youtubeDownloader:
    """
The main class
    """

    def __init__(self, url):
        self.url = url
        self.video = new(self.url)
        self.stats = self.displayStats

    @property
    def displayStats(self) -> dict:
        return {
            "author": self.video.author,
            "title": self.video.title,
            "duration": self.video.duration,
            "viewcount": f"{self.video.viewcount / 1_000_000}M",
        }

    def pprintStats(self) -> None:
        pprint(self.stats)

    def Downloadvideo(self):
        createdir("./videos°^°")
        self.video.getbestvideo().download()

    def DownloadAudio(self):
        createdir("./music°^°")
        self.video.getbestaudio().download()

    @property
    def getStreams(self) -> list:
        return self.video.allstreams

    def downloadbyIndex(self):
        for i, value in enumerate(self.getStreams):
            print(
                f"{i} \t ===> {value} ==> \t {value.get_filesize()}"
            )
        index = int(input('choose the stream you want ammongs the numbers above : \t'))
        self.getStreams[int(index)].download()


url = input("  paste the Link : \t\t")
res = youtubeDownloader(url)
sleep(3)
print("video Information: ")
print(f"title ==> {res.stats['title']}\n "
      f"author ==> {res.stats['author']}")
Format = int(input(" (0) ==> audio\n"
                   " (1) ==> video\n"
                   ))
if Format == 0:
    """
      in case we are looking for an audio we would take a look at:
      res.getStreams, which returns every stream that we can download then
      we can filter out what we don't want.
      then we can ask the user to choose the desired format. =>  webm, mp3, m4a (size included) 
      """
    filteredAudio = [i for i in res.getStreams if not str(i).startswith('video') and not str(i).startswith('normal')]
    for i, value in enumerate(filteredAudio):
        print(f"\t{i}  {value} {int(value.get_filesize()) * 10 ** -6 // 1} MB")
    index = int(input("  Choose a quality : ==> "))
    print("processing....")
    sleep(2)
    a = time()
    filteredAudio[index].download()
    b = time()
    print(f"  took about {b - a}s which is {(b - a) // 60} min")
elif Format == 1:
    filteredVideo = [i for i in res.getStreams if not str(i).startswith('audio')]
    for i, value in enumerate(filteredVideo):
        print(f"{i}  {value} {int(value.get_filesize()) * 10 ** -6 // 1} MB")
    index = int(input("choose a quality : ==> "))
    print("processing..")
    sleep(2)
    a = time()
    filteredVideo[index].download()
    b = time()
    print(f"took about {(b - a)}s which is  {(b - a) // 60} min")
else:
    print("something went wrong!!!")

print("Scripted by : Moody :)")
sleep(0.5)
print("github: github.com/Moody0101")
sleep(0.5)
print("email : azmoudh@gmail.com")

