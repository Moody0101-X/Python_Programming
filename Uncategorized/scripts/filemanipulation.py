"""
this module is a folder organizer to initialize ==> x = organizer(), then
you either specify the root or you specify the files path.
when root is none then root = os.getcwd(), it also automatically outputs the files in the derictory
and it can filter with the filter function (filter(ext: str)).
note : this works with another module of mine that wraps up the getting file names operation(by inheritence btw).
"""
import pprint
import os
import shutil
# from os import path
from dataclasses import dataclass


@dataclass
class fileList(object):
    """
    this class is used to find files in a specified path, that has a specified extension
    however if you don't specify it will return the whole dir files.
    if you specify all then it will return every file in the dir
    """

    def __init__(self, path=None):
        self.path: str = path
        self.fileList: list = list()

    def __str__(self):
        return str(self.fileList)

    def __len__(self):
        return len(self.fileList)

    def generator(self, ext=None, only_folders=False) -> list:
        """
        file list generator
        """
        if self.path is None:
            self.path = str(os.getcwd())
        for i in os.scandir(self.path):
            if ext is not None and only_folders is False:
                if i.name.endswith(ext):
                    file = i.name
                    self.fileList.append(file)
            elif ext is None and only_folders is True:
                file = i.name
                self.fileList.append(file)
                for item in self.fileList:
                    if '.' in item:
                        self.fileList.remove(item)
            else:
                file = i.name
                self.fileList.append(file)
        return self.fileList

    def walk(self) -> dict:
        """
        class recursion,  I mean that is kinda cool
        :return: a list of files in the subdirectories
        """
        l = {}

        for i in self.generator(only_folders=True):
            sub = fileList(self.path + "/" + str(i))
            l[i] = sub.generator()
        return l


@dataclass
class organizer(fileList):
    root: str = None
    if root is None:
        root = os.getcwd()
    files = fileList(root).generator()

    def __init__(self):
        super(organizer, self).__init__()
        self.filteredList = []

    def filter(self, ext: str) -> list:
        i = 0
        while i < len(self.files):
            if not self.files[i].endswith(str(ext)):
                pass
                i += 1
            else:
                self.filteredList.append(self.files[i])
                i += 1
        return self.filteredList

    def makeandmove(self, path0: str, i) -> None:

        abspath = self.root + "\\" + path0
        if not os.path.exists(abspath):
            os.makedirs(abspath)
        shutil.move(self.root + "/" + str(i), abspath)

    def organize(self) -> bool:

        for i in self.files:
            if i.endswith("mp3") or i.endswith("wave"):
                self.makeandmove('audio', i)
            elif i.endswith("mp4"):
                self.makeandmove('videos', i)
            elif i.endswith("py"):
                self.makeandmove('python-files', i)
            elif i.endswith("txt"):
                self.makeandmove("text-files", i)
            elif i.endswith("cpp"):
                self.makeandmove("c++", i)
            elif i.endswith("ai"):
                self.makeandmove("adobe-illustrator", i)
            elif i.endswith("json"):
                self.makeandmove("json-s", i)
            elif i.endswith("png") or i.endswith("jpg"):
                self.makeandmove('pictures', i)
            else:
                self.makeandmove('other-ext', i)
        return True

# debugging
if __name__ == '__main__':
    """print([i.name for  i in os.scandir("c://users/pc/documents/My projects/Moody_-")]
          )"""
    x = fileList()
    pprint.pprint(x.walk())
    """
     it returns a map for the current dir if self.path not specified
    
    """