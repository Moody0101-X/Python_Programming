from pafy import new
import requests
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
            "viewcount": self.video.viewcount,
            "allStreams": self.video.allstreams
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


class downloadData:
    def __init__(self, url: str, fileName: str):
        self.url = url
        self.fileName: str = fileName
        self.req = requests.get(url)

    def downloadbinary(self):

        createdir("downloads")
        with open(self.fileName, "+wb") as f:
            f.write(self.req.content)

    def downloaddata(self):
        createdir("downloads")
        with open(self.fileName, "+w") as f:
            f.write(self.req.content)


# debugging
