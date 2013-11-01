import time
import boto.swf.layer2 as swf
import json
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError

class Base(swf.ActivityWorker):

    name = None
    version   = '0.1'
    task_list = None

    def run(self):
        print "%s polling for work" % self.name

        activity_task = self.poll()
        if 'activityId' not in activity_task: 
            print "%s poll time out" % self.name
            return True

        try:
            print 'working on activity from tasklist %s at %i' % (
                    self.task_list, time.time())

            self.activity(activity_task.get('input'))
        except Exception, error:
            self.fail(reason=str(error))
            raise error

        return True


    def activity(self, input):
        raise NotImplementedError

class CreateCombineList(Base):
    """
    Takes a JSON string of: 
        in:
            bucket: "String"
            prefix: "String"
        out:
            bucket: "String"
    """

    name      = 'CreateCombineList'
    version   = '0.1'
    task_list = 'CreateList'

    def activity(self, inputJSON):
        try:
            input = json.loads(inputJSON)
            prefix       = input["prefix"]
            sourceBucket = input["sourceBucket"]
            outputBucket = input["outputBucket"]

            conn = S3Connection()
            print "fetching object list w/ prefix: %s" % prefix

            sBucket = conn.get_bucket(sourceBucket)
            rs = sBucket.list(prefix)

            filelist = {
                "prefix"    : prefix
                , "objects" : []
            }
            for k in rs:    # k is of type boto.s3.key.Key
                filelist['objects'].append({
                    "key" : k.name
                    , "etag" : k.etag[1:-1] # strip quotes
                    , "size" : k.size
                })

            numObjs = len(filelist["objects"])

            # fail the job with reason="no-files"
            if numObjs == 0:
                print "Nothing to do. Failing Task"
                return self.fail(reason="no-files")

            data = json.dumps(filelist) 

            dBucket = conn.get_bucket(outputBucket)
            outputKey = 'filelists/%s-%s.json' % (prefix, time.time())

            print "Uploading: %s, %d objects, filesize: %d bytes" % (outputKey, numObjs, len(data))

            key = Key(dBucket, outputKey)
            key.set_contents_from_string(data)

            input["outputKey"] = outputKey

            self.complete(result=json.dumps(input))
        except (ValueError, TypeError, KeyError), error:
            self.fail(reason="bad-json-input", details=str(error))

        except S3ResponseError, er:
            self.fail(reason=er.code, details=er.body)

class CombineSourceObjects(Base):
    """
    Takes a path to a file list and: 

    1. downloads it
    2. fetches all the referenced objects (in parallel)
    3. creates multi-upload parts from smaller objects
    4. uploads parts

    It is also smart enough to determine where the process left 
    off and resume it if possible.
    """
    name      = 'CombineSourceObjects'
    task_list = 'CombineObjects'
    version   = '0.1'

    def activity(self, input):
        self.complete(result='done')

