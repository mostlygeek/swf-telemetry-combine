#!/usr/bin/env node

var AWS = require("aws-sdk")
    , async = require('async')
    , fetchFragmentList = require("./lib/combiner/fetchFragmentList")
    , CombinedObject = require('./lib/combiner/CombinedObject')
    ;
    
AWS.config.update({
    accessKeyId : process.env.AWS_ACCESS_KEY,
    secretAccessKey : process.env.AWS_SECRET_KEY
});

//AWS.config.update({region: 'us-east-1'});
AWS.config.apiVersions = { s3 : '2006-03-01' };

var s3 = new AWS.S3();


var sourceBucket = 'telemetry-published-v1' 
    , bucket = 'telemetry-test-bucket'
    , key = 'filelists/idle_daily/Firefox/nightly/27.0a1/20130918030202.201310-1383156161.32.json'
    , workDir = '/tmp/work';

fetchFragmentList(s3, bucket, key, function(err, fragList) {
    if (err) return console.log(err);
    console.log(fragList.length);

    // create combined files locally
    var fragment
        , combinedObjs = [new CombinedObject(s3, sourceBucket, workDir, 0)]
        , cObj = combinedObjs[0];
    
    for (i=0; i<fragList.length; i++) {
        fragment = fragList[i];

        if (cObj.addFragment(fragment) === false) { // It's FULL!
            // make a new one
            cObj = new CombinedObject(s3, bucket, workDir, combinedObjs.length);
            cObj.addFragment(fragment);
            combinedObjs.push(cObj);
        }
    }

    combinedObjs.forEach(function(obj, i) {
        obj.debugPrint();
    });

    /*
     * Start uploading
     */
    var uploadQueue = async.queue(function(combinedObj, doneCB) {
        combinedObj.upload(bucket, 'test-' + combinedObj.num, function(err, result) {
            doneCB();
        });
    }, 1); // do 1 at a time

    uploadQueue.push(combinedObjs);
});
