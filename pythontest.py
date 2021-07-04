import threading
from queue import PriorityQueue, Empty
import time
import logging


class ProcessFilesThread(threading.Thread):
    def __init__(self, file1, file2, out_queue) -> None:
        super().__init__()
        self.file1 = file1
        self.file2 = file2
        self.out_queue = out_queue

    def run(self):
        with open(self.file1, 'r') as file_1, open(self.file2, 'r') as file_2:
            chunk = []
            chunksize = 100000
            chunk_num = 0
            position = 0
            for x, y in zip(file_1, file_2):
                chunk.append((int(x), int(y)))
                if position < chunksize:
                    position += 1
                else:
                    self.out_queue.put((chunk_num, chunk))
                    chunk = []
                    chunk_num += 1
                    position = 0
            self.out_queue.put((chunk_num, chunk))


class ProcessThread(threading.Thread):
    def __init__(self, in_queue, out_queue) -> None:
        super().__init__()
        self.in_queue = in_queue
        self.out_queue = out_queue

    def run(self):
        while True:
            try:
                chunk = self.in_queue.get_nowait()
                results = []
                for line in chunk[1]:
                    results.append(line[0]+line[1])
                self.out_queue.put((chunk[0], results))
                self.in_queue.task_done()
            except Empty:
                break


class SaveChunk(threading.Thread):
    def __init__(self, processed_queue) -> None:
        super().__init__()
        self.work_queue = processed_queue

    def run(self):
        with open("outfile.txt", "w") as out_file:
            while True:
                try:
                    chunk = self.work_queue.get(block=True, timeout=20)
                    out_file.writelines("%s\n" % num for num in chunk[1])
                    self.work_queue.task_done()
                except Empty:
                    break


def WordCount(file_name):
    count = 0
    for line in open(file_name):
        count += 1
    return count


if __name__ == "__main__":

    start_time = time.time()
    # File Processing Queue
    file_queue = PriorityQueue()
    # Queue holding results after addition before file writing
    results_queue = PriorityQueue()

    t = ProcessFilesThread("/Users/david/Google Drive/Personal/Education/CSU-Global/CSC507 - Foundations of Operating Systems/Module 8/CSC507_PortfolioProject/hugefile1.txt",
                           "/Users/david/Google Drive/Personal/Education/CSU-Global/CSC507 - Foundations of Operating Systems/Module 8/CSC507_PortfolioProject/hugefile2.txt", file_queue)
    t.setDaemon(True)
    t.start()
    t.join()

    # Thread pool
    threads = []

    # Create 1 thread per processor
    for _ in range(8):
        t = ProcessThread(file_queue, results_queue)
        threads.append(t)
        t.setDaemon(True)
        t.start()

    # # Wait until all threads are complete
    # for t in threads:
    #     t.join()

    save_thread = SaveChunk(results_queue)
    save_thread.setDaemon(True)
    save_thread.start()

    save_thread.join()

    end_time = time.time()

    print("Resultant Word Count: {}".format(WordCount("outfile.txt")))
    print("Time Taken: {}".format(end_time - start_time))
