
from concurrent.futures import process
from io import TextIOWrapper
from os import stat
from time import sleep
# Use these constants to read as bytes or as lines using the FileLoader.
LINES = 0
BYTES = 1


CSVFILES = [
    open("C:\\Users\\Hossin\\Desktop\\Data\\DataWithoutEmails.csv", "a"),
    open("C:\\Users\\Hossin\\Desktop\\Data\\DataWithEmails.csv", "a")
]


class FileLoader:
    """ 
        Class to work with chunks of data read from a file.
        Use cases:
            - sending files through a network.
            - processing files.
            - File Encryption.
    """

    def __init__(self, fp: TextIOWrapper, chunk_size: int = 1024, unit: str = "KB", thresh_hold: int = 0, procedure: int = LINES) -> None:
        
        self.DataMap = {
            "Byte": 1,
            "KB": 1024 ** 1,
            "MB": 1024 ** 2,
            "GB": 1024 ** 3,
            "TB": 1024 ** 4
        }

        self.fp = fp
        self.chunk_size = chunk_size
        self.unit = unit
        self.thresh_hold = thresh_hold
        self.procedure = procedure
        
        self.fileSize = {
            'size': stat(self.fp.name).st_size / self.DataMap[unit],
            'Unit': unit
        }
    
    def LoadFileLines(self):
        """ loads chunk of lines, untill threshHold is met. """
        temp = 0
        ChunkSizeInBytes = self.DataMap[self.unit] * self.chunk_size
        while True:
            Bytes = self.fp.readlines(ChunkSizeInBytes)
            if temp <= self.thresh_hold:

                if Bytes:
                    yield Bytes
                    temp += self.chunk_size
                            
                else:
                    self.fp.close()
                    return
            else:
                print()
                print(f"{temp} {self.unit} was loaded succeffuly.")
                self.fp.close()
                return

    def LoadFileBytes(self):
        """ loads chunk of Bytes, untill threshHold is met. """
        temp = 0
        ChunkSizeInBytes = self.DataMap[self.unit] * self.chunk_size
        while True:
            Bytes = self.fp.read(ChunkSizeInBytes)
            if temp <= self.thresh_hold:

                if Bytes:
                    yield Bytes
                    temp += self.chunk_size
                            
                else:
                    self.fp.close()
                    return
            else:
                print()
                print(f"{temp} {self.unit} was loaded succeffuly.")
                self.fp.close()
                return
    
    def ProcessAll(self, callback: callable = False, progress: bool = False):
        """ Function to process the whole file. but still in chunks of size n. """
        self.thresh_hold = self.fileSize['size']
        self.Process(
            callback=callback
        )

    def Process(self, callback: callable = False, CleanUp: callable = None):
        """ 
            Give it a callback to deal with each chunck of data. 
            to encode then send:
                >>> def encode_the_send(data): send(b64encode(data.encode()).decode())
                >>> created_fileLoader_instance.Process(encode_the_send)
            The example above is a perfect example for a use case.
            :param callback: Func to call on the chunk.
            :param progress: Bool to show or hide chunk processing progress.
        """
        processed = 0
        ThinKB = self.thresh_hold * self.DataMap[self.unit] / self.DataMap["KB"]
        
        if self.procedure == LINES:
            generatorOBJ = self.LoadFileLines()
        elif self.procedure == BYTES:
            generatorOBJ = self.LoadFileBytes()
        else:
            generatorOBJ = self.LoadFileBytes()

        for i in generatorOBJ:    
            # Takes every chunk and perform the callback.
            
            if callback:
                callback(i)
            
            temp = int(processed * self.DataMap[self.unit]) / self.DataMap["KB"]
            
            processed +=  self.chunk_size
            print(processed, end="\r")

        print("Finished...")

def main1():
    """ Example1. """
    file = open("file.txt", "r")
    
    fileloader = FileLoader(
        file,
        chunk_size = 1,
        unit="MB",
        thresh_hold=89
    )

    print(fileloader.fileSize)

    fileloader.ProcessAll(
        lambda x: print(x),
        progress=True
    )

def main2():
    """ Example2. """
    file = open("file.txt", "r")
    
    fileloader = FileLoader(
        file,
        chunk_size = 1,
        unit="MB",
        thresh_hold=89
    )

    print(fileloader.fileSize)

    fileloader.ProcessAll(
        lambda x: print(x),
        progress=True
    )


def LoadMyDB():
    """ Example2. """
    file = open("C:\\Users\\Hossin\\Desktop\\Data\\Morocco\\morcoo full public.txt", "r", encoding="utf-16")
    
    fileloader = FileLoader(
        file,
        chunk_size = 1,
        unit="MB",
        thresh_hold=1200
    )

    print(fileloader.fileSize)

    fileloader.Process(
        getAndProcessData
    )


def getAndProcessData(dataFrame):
    """ write certain data format in some csv files.
    ID, NUMBER, FULLNAME, EMAIL, HOMETOWN
    ID, PHONE, FULLNAME, HOMETOWN

     """
    index = 0
    data = None

    for row in dataFrame:
        newDataFrame = row.split(",")
        if newDataFrame[4] != "None": 
            data = f"{newDataFrame[0]}, {newDataFrame[1]}, {newDataFrame[2] + newDataFrame[3]}, {newDataFrame[4]}, {newDataFrame[6]}, {newDataFrame[8]}\n"
            index = 1
        else:
            data = f"{newDataFrame[0]}, {newDataFrame[1]}, {newDataFrame[2] + newDataFrame[3]}, {newDataFrame[6]}, {newDataFrame[8]}\n"
            index = 0
        try:
            CSVFILES[index].write(data)
        except:
            pass





        # sleep(.1)


def TestData(dataFrame):
    """ 
        test function to see the format of the data
    """

    index = 0
    data = None

    for row in dataFrame:
        newDataFrame = row.split(",")
        for i, v in enumerate(newDataFrame):
            print(i, v)
        print()
        sleep(.1)


if __name__ == "__main__": LoadMyDB()











