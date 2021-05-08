import sys
import re


def process_file(fname):
    with open(fname, "r") as f:
        print('done')
        while f.readline(1) == '#':
            pass
        print('done')
        while True:
            c = f.read(8192)
            print(c)
            if c == '':
                return

            c = re.sub(r'\t', ' ', c)
            sys.stdout.write(c)


if __name__ == '__main__':
    process_file(sys.argv[1])
