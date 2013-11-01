import time
import threading

from random import random
from Queue import Queue,Empty
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError

class Downloader(threading.Thread):
    def __init__(self, threadID, outputDir, workQueue, runEvent):
        threading.Thread.__init__(self)
        self.name = "thread-%s" % threadID

        self.outputDir = outputDir
        self.workQueue = workQueue
        self.runEvent = runEvent

        self.conn = S3Connection()
        self.sourceBucket = self.conn.get_bucket('telemetry-published-v1')

    def run(self):

        # keep running until queue is empty or we're all 
        # done
        while self.runEvent.is_set():
            time.sleep(0.100)
            try:
                item = self.workQueue.get_nowait()
                path,etag = item.split(',')
                output = "%s/%s.frag" % (self.outputDir, etag)
                f = Key(self.sourceBucket, path)
                f.get_contents_to_filename(output)
                #print "thread: %s, downloaded etag: %s" % (self.name, etag)
            except Empty: 
                break

class ParallelDownloader:
    def __init__(self, taskList, outputDir,workers=5):
        self.outputDir = outputDir
        self.downloadQueue = Queue()
        self.workers = workers
        self.runEvent = threading.Event()

        for o in taskList: 
            self.downloadQueue.put(o)

    def start(self): 
        downloaders = []
        self.runEvent.set()
        for threadId in range(0, self.workers):
            d = Downloader(threadId
                    , self.outputDir
                    , self.downloadQueue
                    , self.runEvent
            )
            d.start()
            downloaders.append(d)

        try:
            last = 0
            while True:
                size = self.downloadQueue.qsize()
                if size != last:
                    print "Queue size: %d (%d)" % (size, last - size)
                    last = size

                if self.downloadQueue.empty():
                    print "Nothing left to do. wait for threads to fin"
                    for d in downloaders:
                        d.join()

                    break
                time.sleep(0.1)
        except KeyboardInterrupt:
            self.runEvent.clear()
            print "Stopping %d threads..." % len(downloaders)
            for d in downloaders:
                d.join()
            print "exiting"

        return True

tmpDir = '/tmp/work'
conn = S3Connection()
sourceBucket = conn.get_bucket('telemetry-published-v1')
listBucket = conn.get_bucket('telemetry-test-bucket')

# this is the big one ... ~2500 items
k = 'filelists/idle_daily/Firefox/nightly/27.0a1/20130918030202.201310-1383083556.14.csv'

# small one, ~ 8 items
#k = 'filelists/idle_daily/Firefox/nightly/27.0a1/20130918030202.20131005.v2.log.7-1383082671.33.csv'

print "Downloading list... " + k
f = Key(listBucket, k)
objs = f.get_contents_as_string()

print "Filling Queue..."
list = objs.split('\n')

print "Starting Parallel Download"
pD = ParallelDownloader(list, tmpDir, workers=20)
pD.start()


# download the file list
# fetch all the fragments in parallel
# organize fragments into parts ~5MB each
# upload parts to make 100MB uploads
# done
