from time import time

"""
a more clean and optamized implementation for my binary search prev code in this repo
this one is more general and it covers how to use binary search with also letters.

"""


class binarySearch:
    def __init__(self, arr: list, data):
        self.arr: list = arr
        self.low: int = 0
        self.high: int = len(self.arr)
        self.data = data

    def intsearch(self) -> list:
        while self.low < self.high:
            middle = (self.low + self.high) // 2
            if self.arr[middle] < self.data:
                self.low = middle + 1
            else:
                self.high = middle
        return self.low
    @property
    def timing(self) -> str:
        t1: float = time()
        self.search()
        t2: float = time()
        return f"{t2 - t1}"


array = [i for i in range(0, 100_000_00)]


def run():
    b = binarySearch(array, 2898932)
    print(b.search())
    print(b.timing)


if __name__ == '__main__':
    run()
