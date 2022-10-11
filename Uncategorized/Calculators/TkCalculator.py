from tkinter import *

root = Tk()
root.frame()
root.title('calculator')
root.configure(background='#2980b9')
e = Entry(root, width=30, borderwidth=3)
e.grid(row=0, column=0, columnspan=3, pady=10, padx=10, ipadx=100, ipady=8)


# create functions

def meth():
    first_number = e.get()
    global f_n
    f_n = int(first_number)
    e.delete(0, END)
def click(number):
    current = e.get()
    e.delete(0, END)
    e.insert(0, str(current) + str(number))


def delete():
    e.delete(-1, 0)



def clear():
    e.delete(0, END)


def add():
    first_number = e.get()
    global f_n
    f_n = int(first_number)
    e.delete(0, END)
    global math
    math = 'addition'
def subtract():
    meth()
    global math
    math = 'subtraction'

def multi():
    meth()
    global math
    math = 'multi'
def equal():
    second_number = e.get()
    e.delete(0, END)
    if math == 'addition':
        e.insert(0, f_n + int(second_number))
    elif math == 'subtraction':
        e.insert(0, f_n - int(second_number))
    else:
        e.insert(0, f_n * int(second_number))

# create Buttons


button1 = Button(root, text='1', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(1))

button2 = Button(root, text='2', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(2))
button3 = Button(root, text='3', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(3))

button4 = Button(root, text='4', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(4))
button5 = Button(root, text='5', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(5))

button6 = Button(root, text='6', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(6))
button7 = Button(root, text='7', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(7))

button8 = Button(root, text='8', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(8))
button9 = Button(root, text='9', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(9))
button0 = Button(root, text='0', padx=40, pady=20, fg='black', bg='#f1c40f', command=lambda: click(0))

button_add = Button(root, text='+', padx=40, pady=20, fg='black', bg='#bdc3c7', command=add)
button_subtract = Button(root, text='-', padx=40, pady=20, fg='black', bg='#bdc3c7', command=subtract)
button_multiplay = Button(root, text='data', padx=40, pady=20, fg='black', bg='#bdc3c7', command=multi)

button_Equal = Button(root, text='=', padx=40, pady=20, fg='white', bg='#2c3e50', command=equal)
button_clear = Button(root, text='Clear', padx=40, pady=20, fg='white', bg='#2c3e50', command=clear)
button_Delete = Button(root, text='delete', padx=40, pady=20, fg='white', bg='#2c3e50', command=delete)

button1.grid(row=1, column=0, padx=5, pady=10)
button2.grid(row=1, column=1, padx=5, pady=10)
button3.grid(row=1, column=2, padx=5, pady=10)

button4.grid(row=2, column=0, padx=5, pady=10)
button5.grid(row=2, column=1, padx=5, pady=10)
button6.grid(row=2, column=2, padx=5, pady=10)

button7.grid(row=3, column=0, padx=5, pady=10)
button8.grid(row=3, column=1, padx=5, pady=10)
button9.grid(row=3, column=2, padx=5, pady=10)

button0.grid(row=4, column=0, padx=5, pady=10)

button_subtract.grid(row=1, column=3, padx=5, pady=10)
button_multiplay.grid(row=2, column=3, padx=5, pady=10)
button_add.grid(row=3, column=3, padx=5, pady=10)

button_Equal.grid(row=4, column=1, padx=5, pady=10)
button_clear.grid(row=7, column=0, columnspan=4, padx=5, pady=10, ipadx=100)
button_Delete.grid(row=4, column=2, columnspan=2, padx=5, pady=10)

root.mainloop()
