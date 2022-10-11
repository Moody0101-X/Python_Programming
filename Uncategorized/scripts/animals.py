"""
a really basic animals guessig game
author: Moody0101
language: python
date: not specified.

"""

from random import choice
class animal:
   
    def __init__(self, guesses, guess_count) -> None:
        self.animals: list =  [
            "cat", "dog",
            "horse", "camel",
            "bird", "penguine", 
            "monkey", "donkey"
        ]
        self.guesses: int = guesses
        self.guess_count: int = guess_count
        
    def play(self) -> None:
        print(f"""
    rules:
    you are gonna guess an animal name,
    the program will pick automaticly from this list of animals

    {self.animals}

    note: you have limited tries. 
    """)
        print()
        animal = choice(self.animals)
        while self.guess_count >= self.guesses:
            print("==================================================================")
            print(" guess the animal")
            print(f" tries left: {self.guess_count + 1 - self.guesses}")

            print("===================================================================")
            print()
            answer = input()
            if answer == animal:
                print("===============================================================")
                print(f" yeaa you win it is {animal}")
                print("===============================================================")
                break
            else:
                self.guesses += 1
        if  self.guess_count == self.guesses - 1:
            print("==================================================================")
            print(f" You just lost it is a {animal}")
            print("==================================================================")


print("ok")
animal(1, 3).play()