#!/usr/bin/env node

/*
 * This clones/copies objects from one bucket to another
 */

var program = require('commander')
    , async = require('async')
    , AWS = require('aws-sdk')
    , debug = require('debug')
    , d = {
        err : debug("error")
        , copy: debug("copy")
        , page: debug("page")
        , dry: debug("copy:dryrun")
    }
    ;

program
    .version("0.0.1")
    .option('--dryrun', 'Don\'t make actual changes', Boolean, false)
    .option('-r, --region <region>', 'AWS region default: us-east-1', String, 'us-east-1' )
    .option('-s, --source <bucket>', 'Source bucket', String, "")
    .option('-p, --prefix <prefix>', 'Prefix for source bucket', String)
    .option('-d, --dest <bucket>', 'Destination bucket', String)
    .parse(process.argv);

var DRY_RUN = (!!program.dryrun);


AWS.config.update({
    accessKeyId : process.env.AWS_ACCESS_KEY,
    secretAccessKey : process.env.AWS_SECRET_KEY
});

AWS.config.update({region: program.region});
AWS.config.apiVersions = { s3 : '2006-03-01' };


var S3 = new AWS.S3();

(function copyObjects(source, prefix, marker, dest) {
    var params = {
        Bucket: source
        , Prefix: prefix
    };
    
    if (marker != null) {
        d.page("Processing Next Page: %s", marker)
        params.Marker = marker;
    }

    S3.listObjects(params, function(err, data) {
        if (err) { console.log(err); return; }

        var numObjects = data.Contents.length,
            numDone = 0, 
            inFlight = 0,
            queueStartTime = Date.now();

        var q = async.queue(function(s3Obj, cb) {
            var sourceKey = program.source + "/" + s3Obj.Key;

            var startTime = Date.now();

            if (DRY_RUN) {
                d.dry("copying: %s", sourceKey);
                setImmediate(cb.bind(this, null));
                return;
            }

            S3.copyObject({
                Bucket: program.dest
                , CopySource: sourceKey
                , Key : s3Obj.Key
            }, function(err, data) {
                if (err) {
                    d.error("%s", err);
                    return cb(err);
                }

                numDone += 1;
                d.copy("done %dms (%d/%d) avg: %d. %s/%s"
                    , (Date.now() - startTime)
                    , numDone
                    , numObjects
                    , Math.round(numObjects / (Date.now() - queueStartTime) * 1000)
                    , program.dest, s3Obj.Key);
                cb(null);
            });
        }, 50);  // S3 seems to have a connection limit

        q.drain = function() {
            if (data.IsTruncated) {
                var marker = data.Contents[data.Contents.length-1].Key;
                copyObjects(source, prefix, marker, dest);
            }
        }

        q.push(data.Contents);

    });
})(program.source, program.prefix, null, program.dest);


