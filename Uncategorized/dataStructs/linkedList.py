"""
-----------------------------------------------------------------------------------------------------------------
=> author: Moody0101
=> date: 16 October 2021
=> description:
    - a linked list that supports getting items by index, length property
    and also you can dump all the data into a json database
    --> the linked list implements alot of methods:
    * dunder methods => [__str__, __repre__,__len__, __iter__, __list__, __getitem__]
    plus, others that are provided with dataclasses lib.
    * other methods:
    => pushLeft
    => pushRight
    => insert
    => remove
    => find
    => store: stores everything in a json database, but you can you a csv file tho.
Note: the prop property in the node class represents a property but you can costumize it whatever you want.
for instance another constructor for a database that stroes usernames and passwrds would be:
    => extend: extending an existing linked list or list
    => reverse: reverse the linked list
    => prettyPrint: traverses all the nodes and prints the properties of the nodes
    beautfully.
    self.data => self.userName
    self.prop  => self.password
I wish you understand.

-----------------------------------------------------------------------------------------------------------------
"""
from pprint import pprint
from random import choice
from string import ascii_letters, digits
from dataclasses import dataclass
from json import dumps, loads
class invalidIndex(Exception):
    pass

@dataclass
class node:
    def __init__(self, data, Next=None, prop=None):
        props = [
        "".join([choice([str(i) for i in choice([ascii_letters, digits])]) for i in range(7)]) for i in range(100)
        ]
        self.data = data
        self.Next = Next
        self.prop = prop
        if self.prop is None:
            self.prop = choice(props)

    def __str__(self):
        return f"({self.data})"

    def __dict__(self):
        return {
        "data": self.data,
        "prop": self.prop,
        "next": self.Next.data
        }

    def asJson(self) -> bytes:
        return dumps(self.__dict__()).encode()


@dataclass
class linkedlist:
    root = None
    size = 0
   
    def __iter__(self):
        current = self.root
        while current:
            yield current
            current = current.Next

    def __str__(self):
        self.nodes = [str(i) for i in self.__iter__()]
        return " => ".join(self.nodes)

    def __len__(self):
        return self.size

    def __list__(self):
        return self.nodes 

    def __getitem__(self, index):
        
        return self.nodes[index]

    def addLeft(self, data):
        if isinstance(data, str) or isinstance(data, int):
            new = node(data, self.root)
            self.root = new
            self.size += 1
        elif isinstance(data, node):
            new = data
            self.root = new
            self.size += 1
        elif isinstance(data, list):
            for _ in data:
                self.addLeft(_)
                self.size += 1

    def addRight(self, data):
        if isinstance(data, list):
            for _ in data:
                self.insert(_, self.size)
        else:
            self.insert(data, self.size)

    def find(self, data):
        prev = None
        current = self.root
        while current:
            if current.data == data:
                return current, prev
            else:
                prev = current
                current = current.Next
                index += 1
        return None, None

    def remove(self, data):
        removing, prevremoving = self.find(data)
        if not removing and not prevremoving: print("The data was not found to be removed")
        else:
            if prevremoving is not None:
                prevremoving.Next = removing.Next
            else:
                self.root = removing.Next
            self.size -= 1
            return True
        return False

    def pop(self):
        prev = None
        current = self.root
        while current:
            if current.Next is None and prev is not None: # if the Node is in the last Node of the list
               prev.Next = current.Next
               self.size -= 1
               return current
            elif current.Next is None and prev is None: # if Node is in root
               prev = self.root
               self.size -= 1
               return current
            prev = current
            current = current.Next
        return False

    def insert(self, data,index):
        if index == 0:
            self.addLeft(data)
        if index < 0:
            raise invalidIndex("Invalid index less than 0!")
        if index > self.size:
            print(f"index {index} is out of range")
        elif not isinstance(index, int):
            raise invalidIndex(f"index = {index}a str or list can not be an index!! ")
        else:
            prev = None
            current = self.root
            position = index
            while position >= 1:
                prev = current
                current = current.Next
                position -= 1
            if not isinstance(data, node):
                new = node(data)
            else:
                new = data
            try:
                prev.Next = new
                new.Next = current
                self.size += 1
            except:
                pass

    def store(self, file: str = "Data.json"):
        current = self.root
        if current == None:
            return False
        table = {}
        while current:
            table[current.data] = current.prop
            current = current.Next
        open(file, "w").write(dumps(table, indent=4))
        print("data dumped")
        return True

    def getData(self, file=None):
        if not file:
            file = "Data.json"
        table = loads(open(file, "r").read())
        pprint(table)

    def reverse(self):
        current = self.root
        prev = None
        while current:
            Next = current.Next # we save the next Node of current
            current.Next = prev # we point the current Node to its previous, we revrse the pointer so to speak
            prev = current # we shift the prev to current which is the node we a are setting on
            current = Next # we shift the current to the next
        self.root = prev




def main():
    l = linkedlist()
    # adding values to the left
    l.addLeft(1)
    l.addLeft(2)
    print(l)
    l.reverse()
    print(l)
def generateFakeData() -> list[node]:
    
    data = [
        "".join([choice([str(i) for i in ascii_letters]) for i in range(7)]) for i in range(100)
    ]
   
    return data

def main2():
    #data = generateFakeData() 
    #print(data)
    l = linkedlist()
    l.addRight([i for i in range(101)])
    print(l)
    l.pop()
    l.pop()
    print(l)
    for _ in range(5):
        l.pop()
    print(str(l))

if __name__ == '__main__':
    main()

