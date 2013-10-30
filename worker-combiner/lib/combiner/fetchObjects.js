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


module.exports = function(s3, bucket, tempDir, fragList, bigDoneCB) {


    function queueWorker(fragment, workerCB) {

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
            if (err) { return workerCB(err); }

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
                        return workerCB(err); 
                    }

                    fs.writeFile(filename, objData.Body, function(err) {
                        if (err) { 
                            debugFetch("ERROR %s", err);
                            return workerCB(err); 
                        }

                        // check the downloaded MD5 to make sure everything is right
                        libfs.md5File(filename, function(md5Hash) {
                            if (md5Hash === etag) {
                                debugFetch("Done: %s (%s)", fragment.key, fragment.etag);
                                workerCB();
                            } else {
                                workerCB();
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
        bigDoneCB();
    };

    // let's get the party started
    queue.push(fragList);
};
