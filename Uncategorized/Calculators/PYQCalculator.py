import PyQt5.QtWidgets as QT
print(dir(QT))

class MainWindow(QT.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Hello')
        self.setLayout(QT.QVBoxLayout())
        self.keypad()
        self.numbers: list = []
        self.operators: list = []
        self.show()

    def numPress(self, n: str):
        self.numbers.append(n)
        self.result.setText(''.join(self.numbers))

    def clear(self):
        self.numbers.clear()
        self.operators.clear()
        self.result.setText('')

    def funcPress(self, o):
        self.numbers.append(o)
        self.result.setText(''.join(self.numbers))

    def r(self):
        result = eval(''.join(self.numbers))
        self.result.setText(str(result))
    def hi(self):
        print("hi")
    def keypad(self):
        container = QT.QWidget()
        container.setLayout(QT.QGridLayout())
        self.result = QT.QLineEdit()
        clear_button = QT.QPushButton('clear', clicked=lambda: self.clear())
        enter_button = QT.QPushButton('=', clicked=lambda: self.r())
        btt9 = QT.QPushButton('9', clicked=lambda: self.numPress('9'))
        btt8 = QT.QPushButton('8', clicked=lambda: self.numPress('8'))
        btt7 = QT.QPushButton('7', clicked=lambda: self.numPress('7'))
        btt6 = QT.QPushButton('6', clicked=lambda: self.numPress('6'))
        btt5 = QT.QPushButton('5', clicked=lambda: self.numPress('5'))
        btt4 = QT.QPushButton('4', clicked=lambda: self.numPress('4'))
        btt3 = QT.QPushButton('3', clicked=lambda: self.numPress('3'))
        btt2 = QT.QPushButton('2', clicked=lambda: self.numPress('2'))
        btt1 = QT.QPushButton('1', clicked=lambda: self.numPress('1'))
        btt0 = QT.QPushButton('0', clicked=self.hi)

        addition = QT.QPushButton('+', clicked=lambda: self.funcPress('+'))
        multi = QT.QPushButton('*', clicked=lambda: self.funcPress('*'))
        subtraction = QT.QPushButton('-', clicked=lambda: self.funcPress('-'))
        devision = QT.QPushButton('/', clicked=lambda: self.funcPress("/"))

        container.layout().addWidget(self.result, 0, 0, 1, 4)
        container.layout().addWidget(enter_button, 1, 0, 1, 2)
        container.layout().addWidget(clear_button, 1, 2, 1, 2)

        container.layout().addWidget(btt9, 2, 0)
        container.layout().addWidget(btt8, 2, 1)
        container.layout().addWidget(btt7, 2, 2)
        container.layout().addWidget(addition, 2, 3)
        container.layout().addWidget(btt6, 3, 0)
        container.layout().addWidget(btt5, 3, 1)
        container.layout().addWidget(btt4, 3, 2)
        container.layout().addWidget(subtraction, 3, 3)
        container.layout().addWidget(btt3, 4, 0)
        container.layout().addWidget(btt2, 4, 1)
        container.layout().addWidget(btt1, 4, 2)
        container.layout().addWidget(multi, 4, 3)
        container.layout().addWidget(btt0, 5, 0, 1, 3)
        container.layout().addWidget(devision, 5, 3)
        self.layout().addWidget(container)


app = QT.QApplication([])
mw = MainWindow()
app.setStyle(QT.QStyleFactory.create('fusion'))
app.exec_()