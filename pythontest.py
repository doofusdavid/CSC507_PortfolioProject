import threading
import time


class WorkPiece:
    def __init__(self, chunk_num, values) -> None:
        self.chunk_num = chunk_num
        self.values = values


class ProcessThread(threading.Thread):
    def __init__(self, results) -> None:
        super().__init__()
        self.in_list = results
        print("Init ProcessThread")

    def run(self):
        while True:
            print(threading.currentThread().getName(), 'Starting')
            if self.in_list:
                chunk = self.in_list.pop(0)
                while chunk.chunk_num != current_chunk:
                    time.sleep(1)
                print("Hi")
                # with open('resultfile.txt', 'a') as file:
                #     file.write('\n'.join(chunk.values) + '\n')


if __name__ == "__main__":
    current_chunk = 0
    chunk1 = WorkPiece(1, ['2', '4', '5', '6'])
    chunk2 = WorkPiece(0, ['2', '4', '2', '1'])

    results = [chunk1, chunk2]

    threads = []
    for i in range(0, 2):
        t = ProcessThread(results)
        threads.append(t)
        t.setDaemon(True)
        t.start()

    while not len(results):
        time.sleep(1)
