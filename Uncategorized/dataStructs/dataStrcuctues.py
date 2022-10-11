"""
my first data structures and algorithms file
* stacks
* Queues
* Maxheaps
* linked list
* binary trees
"""
from collections import deque


class Stack(object):
    """
    the stack implementation
    """

    def __init__(self):
        self.stack = list()
        self.mid: int = len(self.stack) // 2

    def pop(self):
        """
        pops the last item and returns it
        :return:
        """
        if len(self.stack) > 0:
            self.stack.pop()
            return self.stack.pop()

    def push(self, data) -> bool:
        """
        appends the data
        :param data: the item to be appended
        :return: if pushed returns True
        """
        if isinstance(data, (str, int)):
            self.stack.append(data)
        elif isinstance(data, list):
            for i in data:
                self.stack.append(i)
        return True

    def __len__(self) -> int:
        return len(self.stack)

    def __repr__(self) -> str:
        return repr(self.stack)

    def peek(self):
        """
        returns the last item
        :return: int, str, None, list, tuple
        """
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    @property
    def last(self) -> int:
        """
        returns the last Item's indx
        :return: int
        """
        if len(self.stack) > 0:
            return self.stack.index(self.stack[-1])

    def __str__(self) -> str:
        return str(self.stack)

    def __getitem__(self, item) -> int:
        return self.stack.index(item)

    def __copy__(self) -> list:
        return self.stack

    def __sizeof__(self) -> int:
        return self.stack.__sizeof__()

    def find(self, data: int) -> int or None:
        """
        uses binary search to find a specified x entry
        :param data: the data to be found
        :return: int
        """
        low = 0
        high = len(self.stack)
        while low < high:
            mid = (low + high) // 2
            if self.stack[mid] < data:
                low = mid + 1
            else:
                high = mid
        if low < len(self.stack):
            if self.stack[low] == data:
                return low
            else:
                return False
        else:
            return False

    def sortStack(self) -> bool:
        """
        it sorts the stack to make finding an Item easier for the find() function
        :return:
        """
        self.stack.sort()
        return True


class Queue(object):
    """
    a Queue's implementation
    """

    def __init__(self):
        self.queue = deque()

    def pushLeft(self, item) -> None:
        """
        pushes the item to the left of the queue
        :param item: list or str or int
        """
        if isinstance(item, list):
            for i in item:
                self.queue.appendleft(i)
        elif isinstance(item, int or str or bool):
            self.queue.appendleft(item)

    def pushRight(self, item) -> None:
        """
        pushes the item to the right of the queue
        :param item: list or str or int
        """
        if isinstance(item, list):
            for i in item:
                self.queue.append(i)
        elif isinstance(item, int or str or bool):
            self.queue.append(item)

    def __copy__(self) -> deque:
        return self.queue

    def __repr__(self) -> str:
        return repr(self.queue)

    def __getitem__(self, item) -> int:
        return self.queue.index(item)

    def __str__(self) -> str:
        return str(self.queue)

    def __len__(self) -> int:
        return len(self.queue)

    def peek(self):
        """
        get the last item and returns it
        :return: last item in the queue
        """
        if len(self.queue) > 0:
            return self.queue[-1]

    def popLeft(self) -> None:
        """
        it takes of the first Item.
        """
        self.queue.popleft()

    def popRight(self) -> None:
        """
        it takes off the last Item
        """
        self.queue.pop()


class Node(object):
    """
    the node that will hold data for linked lists
    """

    def __init__(self, d, n=None, p=None):
        self.data = d
        self.next = n
        self.prev = p

    def __str__(self):
        return '(' + self.data + ')'


class linkedList(object):
    """"
    linked lists
    """

    def __init__(self, r):
        self.root = r
        self.size = 0

    def add(self, data):
        """
        adding data to the linked list
        :param data: the data to be added
        :return: None
        """
        new_node = Node(data, self.root)
        self.root = new_node
        self.size += 1

    def find(self, d):
        """
        finding an element
        :param d:
        :return:
        """
        this_node = self.root
        while this_node is not None:
            if this_node == d:
                return d
            else:
                this_node = this_node.next

    def remove(self, d):
        """
        the remove operation
        :param d: data to be removed
        :return: None
        """
        this_node = self.root
        prev = None
        while this_node is not None:
            if this_node.data == d:
                if prev is not None:
                    prev.next = this_node.next
                else:
                    self.root = this_node.next
                    self.size += 1
            else:
                this_node = this_node.next
                prev = this_node

        return None

    def printList(self) -> str:
        """
        printing the list
        :return: None
        """
        this_node = self.root
        pres = ""
        while this_node is not None:
            if isinstance(this_node, Node):
                string = "{0}==>".format(str(this_node.data))
                pres.append(string)
                print(string)
                this_node = this_node.next
        return pres


class maxheap:

    def __init__(self, heapList: list):
        super(maxheap, self).__init__()
        self._heap = [0]
        self.size: int = 0
        for item in heapList:
            self.push(item)
            self.size += 1

    def __len__(self):
        return len(self._heap)

    def __str__(self):
        ourHeap = self._heap
        ourHeap.pop(0)
        return str(ourHeap)

    @property
    def last(self) -> int:
        return int(len(self._heap) - 1)

    def push(self, data):
        self._heap.append(data)
        self._bubbleUp(self.last)

    def peek(self):
        if self._heap[1]:
            return self._heap[1]
        else:
            return False

    def pop(self) -> int:
        if len(self._heap) > 2:
            self.swap(len(self._heap) - 1, 1)
            thePop = self._heap.pop()
            self._bubbleDown(1)
            self.size -= 1
        elif len(self._heap) == 2:
            thePop = self._heap.pop()
        else:
            thePop = False
        return thePop

    def swap(self, i, j):
        self._heap[i], self._heap[j] = self._heap[j], self._heap[i]

    def _bubbleUp(self, index):
        parent = index // 2
        if parent < 1:
            return
        if index % 2 == 0:
            if self._heap[parent] < self._heap[index]:
                self.swap(index, parent)
                self._bubbleUp(parent)
        elif index % 2 != 0:
            node = index - 1
            if self._heap[index] > self._heap[node]:
                self.swap(index, node)
                self._bubbleUp(node)

    def _bubbleDown(self, index: int):
        firstChild = index * 2
        secondChild = index * 2 + 1
        largest = index
        if len(self._heap) > firstChild and self._heap[largest] < self._heap[firstChild]:
            largest = firstChild
        if len(self._heap) > secondChild and self._heap[largest] < self._heap[secondChild]:
            largest = secondChild
        if largest != index:
            self.swap(index, largest)
            self._bubbleDown(largest)


# node class
class TreeNode:
    """
    binary tree implementation
    """
    def __init__(self, data) -> None:
        # left child
        self.left = None
        # right child
        self.right = None
        # node's value
        self.data = data

    # print function
    def PrintTree(self):
        print(self.data)

root = TreeNode(27)

root.PrintTree()


x = [i + 5//4 for i in range(0, 11)]
"""from random import shuffle

shuffle(x)
"""


def linked():
    """
    liked lists
    :return: none
    """

    p = maxheap(x)
    p.push(3)
    print(p)


if __name__ == '__main__':
   linked()
