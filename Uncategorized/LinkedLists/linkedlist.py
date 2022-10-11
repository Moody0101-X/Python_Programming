from random import choice
from json  import dumps, loads
# from typing import union
from os import path
"""

"""

class node:

    """
    a node that contains:
        username
        password
        next_pointer
    """
    def __init__(self, userName: str, password, Next = None):
        self.password = password
        self.userName = userName
        self.Next = Next
    def __dict__(self):
        return {
        'user': self.userName,
        'password': self.password
            }
    def __str__(self):
        return str(self.__dict__())
    def __repr(self):
        return  str(self.__dict__())
        
class singlyLinkedList:
    def __init__(self, items: list = None):
        self.head = None
        self.size = 0
        if items is not None:
            for _ in items:
                self.push(_)
    def __len__(self):
        return self.size
    def __iter__(self):
        current = self.head
        while current:
            yield current
            current = current.Next
    def reverse(self):
        prev = None                         
        current = self.head             
        while current:
            Next = current.Next
            current.Next = prev
            prev = current
            current = Next
        self.head = prev
    def push(self, data):
        new = node(Next=self.head, userName=self.size, password=data)
        self.head = new
        self.size += 1
    def printlist(self):
        current = self.head
        nodeList = []
        while current:
            nodeList.append(f'({str(current.userName)})')
            current = current.Next
        return ' => '.join(nodeList)

    def remove(self, data):
        current = self.head
        prev = None
        while current:
            if current.password == data and prev is None:
                current.Next = None
                return True
            elif current.data == password and prev:
                prev.Next = current.Next
                return True
            else:
                prev = current
                current = current.Next
        return False
    def __repr__(self):
        return f'<singlyList, size = {self.size}/>'
                
    def find(self, data):
        current = self.head
        while current:
            if current.userName == data:
                return current
            else:
                current = current.Next
        return 0
    def insert(self, data, index):
        if index == 0:
            self.push(data)
        elif index > self.size - 1:
            return False
        else:
            new = node(data)
            current = self.head
            position = 1
            while position != index:
                current = current.Next
                position += 1
            prev = current
            Next = current.Next
            prev.Next = new
            new.Next = Next
def randpass() -> singlyLinkedList:
    data: singlyLinkedList = singlyLinkedList() 
    word = ''
    chars = 'a b c d e f g h i j k l m n e p k r s t u v w x y z'.split(' ')
    while len(data) < 1000:
        while len(word) <= 6:
            word += choice(chars)
        data.push(word)
        word = ''
    return data

db = "./db.json"


if not path.exists(db):
    name = db
    x = randpass()
    passwords = [_.password for _ in x]
    users = [_.userName for _ in x]
    table = {
    "passwords": passwords,
    "users": users
    }
    open(db, "w+").write(dumps(table))
else:
    pass


