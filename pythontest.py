import threading
import time
from queue import Queue, PriorityQueue


# class WorkPiece:
#     def __init__(self, chunk_num, values) -> None:
#         self.chunk_num = chunk_num
#         self.values = values


class ProcessThread(threading.Thread):
    def __init__(self, results) -> None:
        super().__init__()
        self.in_list = results
        print("Init ProcessThread")

    def run(self):
        while True:
            try:
                chunk = self.in_list.get(block=True, timeout=1)
                print('\n'.join(chunk[1]) + '\n')
                self.in_list.task_done()
            except Queue.Empty:
                break
            # with open('resultfile.txt', 'a') as file:
            #     file.write('\n'.join(chunk.values) + '\n')


if __name__ == "__main__":
    current_chunk = 0
    chunk1 = (1, ['2', '4', '5', '6'])
    chunk2 = (0, ['2', '4', '2', '1'])

    results = Queue()
    results.put(chunk1)
    results.put(chunk2)

    threads = []
    for i in range(0, 2):
        t = ProcessThread(results)
        threads.append(t)
        t.setDaemon(True)
        t.start()

    results.join()
