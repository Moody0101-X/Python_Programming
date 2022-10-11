class reveslist:

    def reverse(self):
        prev = None #initializing the previous node                       
        current = self.head #initializing the head
        while current: # if the current node is valid
            Next = current.Next # go to the next node of the current which is initialy the head
            current.Next = prev # change current's next to point to the prev
            prev = current # 
            current = Next
        self.head = prev


class linkedlist:

    def __iter__(self):
        self.head = None
        return self

    def __next__(self):
        num = self.num
        self.num += 2
        return num