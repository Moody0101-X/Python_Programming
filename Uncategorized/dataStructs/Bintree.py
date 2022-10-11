 
"""

Tree => data, left: Tree, right: Tree
challenge: make a BST, with no toturials and no help.
"""
from string import ascii_letters
from random import shuffle
from multiprocessing import Process


""" First implementation """

class bintree:
    def __init__(self, head):
        
        self.head = head
        self.right = None
        self.left = None

    def __len__(self):
        return len(self.IOT())
    def addElement(self, data):
        
        if data == self.head:
                return

        elif data < self.head:
            if self.left:
                self.left.addElement(data)
            else:
                self.left = bintree(data)
                
        else:
            if self.right:
                self.right.addElement(data)
            else:
                self.right = bintree(data)
                
        

    def IOT(self):

        """ We start with the left subtree then the root then the right """

        elements = []
        if self.left:
            elements.extend(self.left.IOT())
        elements.append(self.head)

        if self.right:
            elements.extend(self.right.IOT())
        return elements


def main(items, head):#{
    tree = bintree(head)
    
    for i in items:
        tree.addElement(i)
    
    print(tree.IOT())
    print(len(tree))
#}

def useProcess(items, letters):

    ps = [Process(target=main, args=(items, 0, )), Process(target=main, args=(letters, 'A', ))]
    for i in ps:
        i.start()
        i.join()

def normal(items, letter):

    main(items, 0)
    main(letter, 'A')


if __name__ == '__main__':
    from time import time
    items = [
        1, 6, 0, 3, 1, 17, 63, 0, 2, 13, 7, -2, -100
    ]
    letters = [i for i in ascii_letters]
    shuffle(letters)
    time2 = time()
    useProcess(items, letters)
    print("the process took => ", time() - time2, "s")

    time1 = time()

    normal(items, letters)
    print("the normal took => ", time() - time1, "s")

""" second implementation"""
class BinTree:


    """
    different traversals!

    (a) Inorder (Left, Root, Right) : 4 2 5 1 3 

    (b) Preorder (Root, Left, Right) : 1 2 4 5 3 

    (c) Postorder (Left, Right, Root) : 4 5 2 3 1

    Breadth-First or Level Order Traversal: 1 2 3 4 5 

    """

    def __init__(self, head):
        self.head = head
        self.right = None
        self.left =  None


    def add(self, data):
        if data == self.head:
            return
        
        elif data > self.head:
            if not self.right:
                self.right = BinTree(data)
            
            else:
                self.right.add(data)
        
        elif data < self.head:
            if not self.left:
                self.left = BinTree(data)
            
            else:
                self.left.add(data)

    def inorderTraversal(self) -> list:
        """(a) Inorder (Left, Root, Right)"""
        nodes = []
        if self.left: 
            nodes += self.left.inorderTraversal()

        nodes.append(self.head)

        if self.right:
            nodes += self.right.inorderTraversal()
        return nodes

    def revInorderTraversal(self):
        """(a) Inorder (right, Root, left)"""
        nodes = []

        if self.right:
            nodes += self.right.revInorderTraversal()

        nodes.append(self.head)

        if self.left:
            nodes += self.left.revInorderTraversal()


        return nodes

    def postorderTraversal(self) -> list:
        """ (b) Postorder (Left, Right, Root) """
        nodes = []
        

        if self.left:
            nodes += self.left.postorderTraversal()
        
        if self.right:
            nodes += self.right.postorderTraversal()

        nodes.append(self.head)     
        return nodes

    def PreorderTraversal(self):
        """ (b) Preorder (Root, Left, Right) """
        nodes = []
        nodes.append(self.head)     
        if self.left:
            nodes += self.left.PreorderTraversal()
        
        if self.right:
            nodes += self.right.PreorderTraversal()
        return nodes

    def getLevel(self) -> int:
        level = 1
        if self.left:
            level += self.left.getLevel()
        if self.right:
            level += self.right.getLevel()
        return level

    def reverse(self):
        
        left = self.left
        self.left = self.right
        self.right = left

        if self.left:
            self.left.reverse()
        if self.right:
            self.right.reverse()
        return 0

    def find(self, d):
        if self.head == d:
            return self.head
        elif d < self.head:
            return self.left.find(d)
        elif d > self.head:
            return self.right.find(d)

""" THird implemention """

class Node:

    def __init__(self, head, left=None, right=None):
        
        self.head = head
        self.left = left
        self.right = right
    
    def getData(self):
        return self.head

    def __str__(self):
        return f"({self.head})"

    def __repr__(self):
        return f"<head: {self.head} left: {self.left} Right: {self.right}>"

    def __add__(self, other):
        if other == self.head:
            return
        elif other < self.head:
            if self.left:
                self.left.__add__(other)
            else:
                self.left = Node(other)
        elif other > self.head:
            if self.right:
                self.right.__add__(other)
            else:
                self.right = Node(other)
            

