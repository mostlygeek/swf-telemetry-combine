var fs = require('fs')
    , libfs = require('./fslib')
    , _ = require("underscore")
    , async = require("async")
    , crypto = require('crypto')
    , debug = require('debug');

var debugInfo = debug("fetchObject")
    , debugFetch = debug("fetchObject:fetch")
    , debugMD5 = debug("fetchObject:MD5")
    , debugSkip  = debug("fetchObject:skip");


/*
 * given a list of S3 Keys, this will downloa them all concurrently 
 * since they are usually quite small < 100K and doing them serially 
 * would be quite slow.
 */
module.exports = function(s3, bucket, tempDir, fragList, bigDoneCB) {


    /*
     * we need *all* the fragments for a job to be successful so 
     * on the first failure. Unfortunately w/ async.queue there 
     * is no way to abort further processing.
     */
    var ERROR_HAPPENED = null;

    function queueWorker(fragment, workerCB) {
        /*
         * there's no way to stop the queue() from flushing so we just don't do
         * anything with other workers. 
         */
        if (ERROR_HAPPENED) {
            setImmediate(workerCB);
            return;
        }

        var etag = fragment.etag;
        var filename = libfs.makeFragName(tempDir, fragment);

        async.auto({
            exists: function(cb) {
                fs.exists(filename, cb.bind(this, null));
            },

            md5OK: ["exists", function(cb, results) {
                if (results.exists === true) {
                    libfs.md5File(filename, function(md5Hash) {
                        if (md5Hash === etag) {
                            cb(null, true);
                        } else {
                            debugMD5("MD5 does not match, etag=%s, md5=%s", etag, md5Hash);
                            cb(null, false);
                        }
                    })
                } else {
                    setImmediate(cb.bind(this, null, false));
                }
            }]
        }, function(err, results) {
            if (err) { 
                ERROR_HAPPENED = err;
                return workerCB(err); 
            }

            if (results.exists === true && results.md5OK === true) {
                debugSkip(filename);
                setImmediate(workerCB.bind(this, null));
            } else {
                /* 
                 * purposely don't work this as a stream as object data is samll 
                 * and buffering it RAM is OK.
                 */
                s3.getObject({Bucket: bucket, Key: fragment.key}, function(err, objData) {
                    if (err) { 
                        debugFetch("S3 getObject ERROR %s, %s", fragment.key, err);
                        ERROR_HAPPENED = err;
                        return workerCB(err); 
                    }

                    fs.writeFile(filename, objData.Body, function(err) {
                        if (err) { 
                            debugFetch("ERROR %s", err);
                            ERROR_HAPPENED = err;
                            return workerCB(err); 
                        }

                        // check the downloaded MD5 to make sure everything is right
                        libfs.md5File(filename, function(md5Hash) {
                            if (md5Hash === etag) {
                                debugFetch("Done: %s (%s)", fragment.key, fragment.etag);
                                workerCB();
                            } else {
                                var err = new Error("MD5/etag mismatch on: " + fragment.key);
                                workerCB(err);
                                ERROR_HAPPENED = err;
                                debugFetch("Error, md5 mismatch %s", fragment.key);
                            }
                        });
                    });
                });
            }
        });
    };

    var queue = new async.queue(queueWorker, 25);
    queue.drain = function() {
        bigDoneCB(ERROR_HAPPENED);
    };

    // let's get the party started
    queue.push(fragList);
};
