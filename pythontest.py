import threading
import time
from queue import Queue, PriorityQueue, Empty
import logging

# class WorkPiece:
#     def __init__(self, chunk_num, values) -> None:
#         self.chunk_num = chunk_num
#         self.values = values
logging.getLogger(__name__)
logging.basicConfig(filename="adder.log", filemode='w',
                    encoding="utf-8", format='%(levelname)s: %(message)s', level=logging.DEBUG)


class ProcessFilesThread(threading.Thread):
    def __init__(self, file1, file2, out_queue) -> None:
        super().__init__()
        self.file1 = file1
        self.file2 = file2
        self.out_queue = out_queue

    def run(self):
        with open(self.file1, 'r') as file_1, open(self.file2, 'r') as file_2:
            chunk = []
            chunksize = 1000
            chunk_num = 0
            position = 0
            for x, y in zip(file_1, file_2):
                if position < chunksize:
                    chunk.append((int(x), int(y)))
                    position += 1
                else:
                    logging.info("End Chunk {}".format(chunk_num))
                    self.out_queue.put((chunk_num, chunk))
                    chunk = []
                    chunk_num += 1
                    position = 0
            self.out_queue.put((chunk_num, chunk))
            # print("chunk {}, value {}".format(chunk_num, chunk[-1]))


# def ProcessFiles(file1, file2):
#     with open(file1, 'r') as file_1, open(file2, 'r') as file_2:
#         chunk = []
#         chunksize = 1000
#         chunk_num = 0
#         position = 0
#         out_queue = PriorityQueue()
#         for x, y in zip(file_1, file_2):
#             if position < chunksize:
#                 chunk.append((int(x), int(y)))
#                 position += 1
#             else:
#                 logging.info("End Chunk {}".format(chunk_num))
#                 out_queue.put((chunk_num, chunk))
#                 chunk = []
#                 chunk_num += 1
#                 position = 0
#     return out_queue


class ProcessThread(threading.Thread):
    def __init__(self, in_queue, out_queue) -> None:
        super().__init__()
        self.in_queue = in_queue
        self.out_queue = out_queue
        logging.info("Init ProcessThread")

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
                    chunk = self.work_queue.get(block=True, timeout=2)
                    out_file.writelines("%s\n" % num for num in chunk[1])
                    print(chunk[0])
                    self.work_queue.task_done()
                except Empty:
                    break


if __name__ == "__main__":

    file_queue = PriorityQueue()

    t = ProcessFilesThread("/Users/david/Google Drive/Personal/Education/CSU-Global/CSC507 - Foundations of Operating Systems/Module 8/CSC507_PortfolioProject/hugefile1.txt",
                           "/Users/david/Google Drive/Personal/Education/CSU-Global/CSC507 - Foundations of Operating Systems/Module 8/CSC507_PortfolioProject/hugefile2.txt", file_queue)
    t.setDaemon(True)
    t.start()
    t.join()
    print(file_queue.qsize())
    results_queue = PriorityQueue()

    threads = []
    for _ in range(8):
        t = ProcessThread(file_queue, results_queue)
        threads.append(t)
        t.setDaemon(True)
        t.start()
        t.join()

    print("Out of process")
    save_thread = SaveChunk(results_queue)
    save_thread.setDaemon(True)
    save_thread.start()

    save_thread.join()
