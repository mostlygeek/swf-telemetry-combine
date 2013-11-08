#!/usr/bin/env node
const FETCH_CONCURRENCY = 15;

var AWS           = require("aws-sdk")
    , fmt         = require('util').format
    , async       = require('async')
    , debug       = require('debug')
    , debugStatus = debug("status")
    , debugError  = debug('error')
    ;

/**
 * A test to see how long it takes to list the entire 
 * idle_daily prefix
 */

AWS.config.update({
    accessKeyId : process.env.AWS_ACCESS_KEY,
    secretAccessKey : process.env.AWS_SECRET_KEY
});

//AWS.config.update({region: 'us-east-1'});
AWS.config.apiVersions = { s3 : '2006-03-01' };

var s3 = new AWS.S3()
    , bucket = "telemetry-published-v1"
    , combinedBucket = "telemetry-test-bucket"
    , itemCount = 0
    , startTime = Date.now()
    , prefix = ''
    ;


list(s3, bucket, prefix, function(count, moreComing) {
    itemCount += count;

    var t = Math.floor((Date.now() - startTime)/1000);

    if (count > 0) {
        var avg = (t > 0) ? Math.round(itemCount/t) : 0;
        console.log(fmt("Got %d, total %d, running: %d seconds (%d/sec)", count, itemCount, t, avg));
    }
});

function list(S3, bucket, prefix, countNotifyCB) {

    var q = async.queue(queueWorker, FETCH_CONCURRENCY);

    function queueWorker(taskPrefix, taskCompleteCB) {

        debugStatus("Fetching prefix: %s", taskPrefix);
        _listObjects(bucket, taskPrefix, null, function(count, more) {

            setImmediate(countNotifyCB.bind(this, count, more));

            if (more == false) {
                taskCompleteCB(null);
            }
        });
    }

    q.drain = function() {
        console.log("done");
    }

    q.push(prefix);

    function _listObjects(bucket, prefix, marker, cb) {
        var params = {
            Bucket: bucket 
            , Prefix : prefix
            , Delimiter : "/" 
        };

        if (!!marker === true) {
            params.Marker = marker;
        }

        debugStatus("%s", JSON.stringify(params));

        S3.listObjects(params, function(err, data) {

            if (err) {
                debugError("Error %s", err);
                return cb(0, false);
            }

            /*
             * add more prefixes to fetch concurrently
             */
            if (data.CommonPrefixes.length > 0) {
                debugStatus("Got %d more prefixes to fetch", data.CommonPrefixes.length);
                for(var cp=0; cp < data.CommonPrefixes.length; cp++) {
                    q.push(data.CommonPrefixes[cp].Prefix);
                }
            }

            count = (data.Contents) ? data.Contents.length : 0;
            if (count > 0) {
                if (data.IsTruncated == true) {
                    var marker = data.Contents[data.Contents.length-1].Key;
                    debugStatus("asking for more with marker: %s", marker);
                    _listObjects(bucket, prefix, marker, cb);
                }

                cb(count, data.IsTruncated);
            } else {
                cb(0, false);
            }
        });
    };
}

