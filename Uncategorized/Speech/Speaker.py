"""
Make text into a speech
lib that I used:
    pyttsx3
"""

import pyttsx3

def main():
    engine = pyttsx3.init()
    text = str(input("what is youn name so I can say it !: "))
    engine.say(text)
    engine.save_to_file(text, "audio.mp3")
    engine.runAndWait()

if __name__ == '__main__':
    main()
