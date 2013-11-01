from time import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError

conn = S3Connection()

prefix = 'idle_daily/Firefox/nightly/27.0a1/20130918030202.20131005.v2.log.7'
#prefix = 'idle_daily/Firefox/nightly/27.0a1/20130918030202.201310'


try:
    bucket = conn.get_bucket('telemetry-published-v1')
    rs = bucket.list(prefix)

    objs = []
    for k in rs:
        objs.append("%s,%s" % (k.name, k.etag[1:-1])) # strip the quotes in the etag

    data = "\n".join(objs)

    destBucket = conn.get_bucket('telemetry-test-bufcket')
    destKey = 'filelists/%s-%s.csv' % (prefix, time())

    print "Uploading new key: %s, %d bytes" % (destKey, len(data))
    k = Key(destBucket, destKey)
    k.set_contents_from_string(data)
    print "Done!"

except S3ResponseError, error: 

    if error.code == "NoSuchBucket":
        print "Boto NoSuchBucket: %s, %s" % (error.code, error.body)
    else:
        print "Boto error: %s" % error.message




