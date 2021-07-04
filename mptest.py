from queue import PriorityQueue, Empty
import time
from multiprocessing import Process
from multiprocessing.managers import SyncManager


def ProcessFilesThread(file1, file2, out_queue):

    with open(file1, 'r') as file_1, open(file2, 'r') as file_2:
        chunk = []
        chunksize = 1000
        chunk_num = 0
        position = 0
        for x, y in zip(file_1, file_2):
            chunk.append((int(x), int(y)))
            if position < chunksize:
                position += 1
            else:
                out_queue.put((chunk_num, chunk))
                chunk = []
                chunk_num += 1
                position = 0
        out_queue.put((chunk_num, chunk))


def ProcessThread(in_queue, out_queue):
    while True:
        try:
            chunk = in_queue.get(block=True, timeout=20)
            results = []
            for line in chunk[1]:
                results.append(line[0]+line[1])
            out_queue.put((chunk[0], results))
            in_queue.task_done()
        except Empty:
            break


def SaveChunk(work_queue):
    with open("outfile.txt", "w") as out_file:
        while True:
            try:
                chunk = work_queue.get(block=True, timeout=2)
                out_file.writelines("%s\n" % num for num in chunk[1])
                work_queue.task_done()
            except Empty:
                break


def WordCount(file_name):
    count = 0
    for line in open(file_name):
        count += 1
    return count


class MyManager(SyncManager):
    pass


def Manager():
    m = MyManager()
    m.start()
    return m


if __name__ == "__main__":
    start_time = time.time()
    # Register a shared PriorityQueue
    MyManager.register("PriorityQueue", PriorityQueue)
    m = Manager()

    file_queue = m.PriorityQueue()
    results_queue = m.PriorityQueue()
    file1 = "/Users/david/Google Drive/Personal/Education/CSU-Global/CSC507 - Foundations of Operating Systems/Module 8/CSC507_PortfolioProject/hugefile1.txt"
    file2 = "/Users/david/Google Drive/Personal/Education/CSU-Global/CSC507 - Foundations of Operating Systems/Module 8/CSC507_PortfolioProject/hugefile2.txt"
    worker_process = Process(target=ProcessFilesThread,
                             args=(file1, file2, file_queue, ))
    worker_process.start()

    time.sleep(5)    # nope, race condition, you shall not pass (probably)
    print("ProcessFilesThread", file_queue.qsize())

    # Proc pool
    procs = []

    # Create 1 thread per processor
    for _ in range(8):
        p = Process(target=ProcessThread, args=(file_queue, results_queue, ))
        procs.append(p)
        p.start()

    print("ProcessThread", results_queue.qsize())
    # # # Wait until all threads are complete
    # # for t in threads:
    # #     t.join()

    save_proc = Process(target=SaveChunk, args=(results_queue,))
    save_proc.start()

    save_proc.join()
    for p in procs:
        p.join()

    end_time = time.time()

    print("Resultant Word Count: {}".format(WordCount("outfile.txt")))
    print("Time Taken: {}".format(end_time - start_time))
