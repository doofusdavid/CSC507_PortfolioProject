import queue
import threading


class SaveThread(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def printfiles(self, p):
        for path, dirs, files in os.walk(p):
            for f in files:
                print(f, file=output)

    def run(self):
        while True:
            result = self.queue.get()
            self.printfiles(result)
            self.queue.task_done()


class ProcessThread(threading.Thread):
    def __init__(self, in_queue, out_queue):
        threading.Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue

    def run(self):
        while True:
            path = self.in_queue.get()
            result = self.process(path)
            self.out_queue.put(result)
            self.in_queue.task_done()

    def process(self, path):
        # Do the processing job here
        pass

        # Create Queue for file reads
pathqueue = queue.Queue()

# Create List for file writes
resultqueue = []


output = codecs.open('file', 'a')

# spawn threads to process
for i in range(0, 5):
    t = ProcessThread(pathqueue, resultqueue)
    t.setDaemon(True)
    t.start()

# spawn threads to print
t = PrintThread(resultqueue)
t.setDaemon(True)
t.start()

# add paths to queue
for path in paths:
    pathqueue.put(path)

# wait for queue to get empty
pathqueue.join()
resultqueue.join()
