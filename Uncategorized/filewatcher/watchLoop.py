"""
as you might have seen, yep this file
will watch the file1.txt and when it changes it will copy everything
to file2.txt
"""
from time import sleep

class watcher:
    def __init__(self, watchedFile: str, outputFile: str):
        self.watchedFile = watchedFile
        self.outputFile = outputFile

    def loop(self):
        loop = True
        while loop:
            with open(f"./{self.watchedFile}", '+r') as watched:
                with open(f"./{self.outputFile}", 'w') as out:
                    out.write(watched.read())
            sleep(2)


# debug
test1 = watcher("file1.txt", "file2.txt")
test1.loop()