#!/usr/bin/env node

var AWS = require("aws-sdk")
    , fetchFragmentList = require("./lib/combiner/fetchFragmentList")
    , fetchObjects = require('./lib/combiner/fetchObjects')
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
    , key = 'filelists/idle_daily/Firefox/nightly/27.0a1/20130918030202.201310-1383156161.32.json';

fetchFragmentList(s3, bucket, key, function(err, fragList) {
    if (err) return console.log(err);
    console.log(fragList.length);

    fetchObjects(s3, sourceBucket, '/tmp/work', fragList, function(err, status) {
        if (err) return console.log(err);
        console.log('done');
    });
});
