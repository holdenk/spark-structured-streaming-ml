import sys
import time

def stream_file(file_path):
    with open(file_path) as f:
        for line in f:
            print(line.strip())
            sys.stdout.flush()
            time.sleep(0.5)

if __name__ == "__main__":
    """
    Stream a file line by line to nc server like so:
        python stream_file.py data_file.txt | nc -lk 9999
    """
    file_path = sys.argv[1]
    stream_file(file_path)