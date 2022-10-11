import argparse

parser = argparse.ArgumentParser()

parser.add_argument("-n1", type=int,
                    help="First number")

parser.add_argument("-n2", type=int,
                    help="Second Number")

args = parser.parse_args()


