import sys
import re


def process_file(fname):
    with open(fname, "r") as f:
        # while True:
        #     line = f.readline()
        #     if not line.startswith("#"):
        #         break
        # while True:
        #     c = f.read(8192)
        #     print(c)
        #     if c == '':
        #         return
        #
        #     c = re.sub(r'[^\d]+]', ' ', c)
        #     sys.stdout.write(c)
        for line in f:
            if not line.startswith("#"):
                sys.stdout.write(line.replace('\t', ' '))


if __name__ == '__main__':
    process_file(sys.argv[1])
