var 
    async       = require("async")
    , fs          = require('fs')
    , fslib       = require('./fslib')
    , debug       = require('debug')
    , debugStatus = debug("multipart:status")
    ;

/* makes a multipart upload callback for async.waterfall below */
function makePartUpload(s3, partNum, filename) {
    var size = fs.statSync(filename).size;

    return function(multipart, cb) {
        var rs = fs.createReadStream(filename);

        fslib.md5File(filename, function(md5Hash) {
            var p;
            for (var i=0; i<multipart.Parts.length; i++) {
                p = multipart.Parts[i];
                if (p.PartNumber === partNum && p.ETag.indexOf(md5Hash) != -1) {
                    debugStatus("Skip Part %d ... already there", partNum);

                    // just stop here
                    return cb(null, multipart);
                }
            }

            debugStatus("Uploading Part %d ...", partNum);
            s3.uploadPart({
                Bucket: multipart.Bucket
                , Key: multipart.Key
                , PartNumber: "" + partNum
                , UploadId : multipart.UploadId
                , ContentLength: size
                , Body: rs
            }, function(err, data) {
                if (err) return cb(err);
                cb(null, multipart);
            });
        });
    }
}

function loadOrCreate(s3, bucket, key, cb) {
    debugStatus("Creating / Fetching Multipart");
    s3.listMultipartUploads({Bucket: bucket, Prefix: key}, function(err, data) {
        if (err) return cb(err);

        if (data.Uploads.length > 0) {
            debugStatus("Found existing upload...");
            var d = data.Uploads[0];
            s3.listParts({
                Bucket: bucket
                , Key: d.Key
                , UploadId: d.UploadId
            }, cb);
            return;
        } 
        
        debugStatus("Creating multi-part upload...");
        s3.createMultipartUpload({Bucket: bucket, Key:key}, function(err, data) {
            if (err) return cb(err);
            data.Parts = [];
            return cb(null, data);
        });
    });
}

function listParts(s3, multipart, cb) {
    s3.listParts({
        Bucket: multipart.Bucket
        , Key: multipart.Key
        , UploadId: multipart.UploadId
    }, function(err, data) {
        if (err) return cb(err);
        cb(null, data);
    });
}

module.exports = function(s3, bucket, key, files, cb) {
    var tasks = [];
    tasks.push(loadOrCreate.bind(this, s3, bucket, key));

    // all the parts to upload... it just takes a list 
    // of files that are on disk
    for(var i=0; i < files.length; i++) {
        // push all the parts
        tasks.push(makePartUpload(s3, i+1, files[i]));
    }

    tasks.push(listParts.bind(this, s3));

    async.waterfall(tasks, function(err, multipart) {
        if (err) {
            debugStatus("ERROR %s", err);
            return cb(err);
        }

        debugStatus("completing MP");
        var parts  = [ ]; 
        multipart.Parts.forEach(function(part) {
            parts.push({ETag: part.ETag, PartNumber: part.PartNumber});
        });

        s3.completeMultipartUpload({
            Bucket: multipart.Bucket
            , Key : multipart.Key
            , UploadId: multipart.UploadId
            , MultipartUpload : {
                Parts: parts
            }
        }, cb);

    });
}
