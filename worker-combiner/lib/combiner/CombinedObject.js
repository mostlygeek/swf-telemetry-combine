const 
    MIN_PART_SIZE = 1024 * 1024 * 5
    , MAX_COMBINED_SIZE = 1024 * 1024 * 50;

var _ = require('underscore') 
    , async = require('async')
    , fetchObjects = require('./fetchObjects')
    , fs = require('fs')
    , fslib = require('./fslib')
    , multipartUpload = require('./multipartUpload')
    , crypto = require('crypto')

    , debug = require('debug')
    , debugStatus     = debug("CombinedObject:status")
    , debugUpload     = debug("CombinedObject:upload")
    , dPartListObject = debug("CombinedObject:partlist:object")
    , dPartListPart   = debug("CombinedObject:partlist:part")
    , dPartListFrag   = debug("CombinedObject:partlist:frag")
    ;

function UploadPart(parent, num) {
    this.parent = parent;
    this.num = num;
    this.size = 0;
    this.fragments = [];
}

UploadPart.prototype.addFragment = function(fragment) {
    this.size += fragment.size;
    this.fragments.push(fragment);

    // continue to update the hash as we add new files 
}

/* an md5 has of all the etags (md5) for all the fragments) */
UploadPart.prototype.getHash = function() {
    var hash = crypto.createHash('md5');

    this.fragments.forEach(function(frag) {
        hash.update(new Buffer(frag.etag, 'hex'));
    });

    return hash.digest('hex');
}

/*
 * Fetch all the fragments from S3 for this part
 */
UploadPart.prototype.fetchFragments = function(cb) {
    var self = this;
    fetchObjects(this.parent.S3, this.parent.sourceBucket, this.parent.workDir, this.fragments, function(err) {
        if (err) return cb(err);

        debugStatus("All fragments fetched for part %d", self.num);
        cb(null, true);
    });
}

UploadPart.prototype.getFilename = function() {
    return this.parent.workDir + "/combined-" + this.getHash() + '.combined';
};

UploadPart.prototype.combineFragments = function(cb) {
    var self = this 
        , filename = self.getFilename();

    fslib.combineFragments(filename, self.parent.workDir, self.fragments, function(err) {
        if (err) {
            debugStatus("Error combining part %d => %s", self.num, err);
            return cb(err);
        } else {
            debugStatus("combined part %d as %s", self.num, filename);
            return cb(null, true);
        }
    });
}

function CombinedObject(S3, sourceBucket, workDir, num) {
    this.workDir = workDir;
    this.S3 = S3;
    this.sourceBucket = sourceBucket;
    this.num = num;
    this.parts = [];
    this.size = 0;
}

/*
 * generates an etag for the final multipart combined object 
 * the S3 one should match, see: 
 *
 * http://permalink.gmane.org/gmane.comp.file-systems.s3.s3tools/583
 *
 */
CombinedObject.prototype.getETag = function(cb) {
    var self = this;
    var totalHash = crypto.createHash('md5');

    /* 
     * looping call back fun-ness
     */
    (function getPartHash(i) {
        if (self.parts[i]) {
            var s = fs.ReadStream(self.parts[i].getFilename());
            var md5 = crypto.createHash('md5');

            s.on('data', function(d) {
                md5.update(d);
            });

            s.on('end', function() {
                totalHash.update(md5.digest('binary'));
                getPartHash(i + 1);
            });
        } else {
            var hex = totalHash.digest('hex');
            cb(hex + '-' + self.parts.length);
        }
    })(0);
}

CombinedObject.prototype.addFragment = function(fragment) {
    if (this.size + fragment.size> MAX_COMBINED_SIZE) {
        return false;
    }

    var part;

    if (this.parts.length === 0) {
        part = new UploadPart(this, 0);
        this.parts.push(part);
    } else {
        part = this.parts[this.parts.length - 1];
    }

    if (part.size > MIN_PART_SIZE) {
        part = new UploadPart(this, this.parts.length);
        this.parts.push(part);
    }

    part.addFragment(fragment);
    this.size += fragment.size;
    return true;
}



/**
 * makes sures we have all the fragments to build the parts
 */
CombinedObject.prototype.fetchFragments = function(cb) {
    var q = async.queue(function(part, workerCB) {
        part.fetchFragments(workerCB);
    }, 5);
    q.push(this.parts);
    q.drain = cb;
};

/* 
 * creates the multipart combined pieces 
 */
CombinedObject.prototype.createCombinedForParts = function(cb) {
    var q = async.queue(function(part, workerCB) {
        part.combineFragments(workerCB);
    }, 1);
    q.push(this.parts);
    q.drain = cb;
}

CombinedObject.prototype.upload = function(destBucket, key, uploadCB) {
    var self = this;

    async.auto({
        "fetch" : self.fetchFragments.bind(self)
        , "combine" : ["fetch", function(cb) { 
            self.createCombinedForParts(function() { 
                cb(null); 
            });  // need better error handling..
        }]
        , "S3ETag": function(cb) {
            self.S3.headObject({Bucket: destBucket, Key: key}, function(err, data) {
                if (data) {
                    var etag = data.ETag.substring(1, data.ETag.length - 1);
                    return cb(null, etag);
                } 

                if (err) {
                    if (err.statusCode == 404) {
                        return cb(null, "");
                    } else {
                        cb(err);
                    }
                }
            });
        }

        , "localETag": ["combine", function(cb) {
            self.getETag(function(etag) { cb(null, etag); });
        }]
        , "upload" : ["localETag", "S3ETag", function(cb, results) {

            if (results.S3ETag == results.localETag) {
                debugUpload("Skip: %s already uploaded", key);
                setImmediate(cb.bind(self, null, true));
                return;
            }

            if (results.S3ETag != '') {
                debugUpload("Re-uploading: %s, ETag mismatch, %s(s3) != %s(local)", 
                        key, results.S3ETag, results.localETag);
            } else {
                debugUpload("Uploading object %s", key);
            }

            var partFiles = [];
            self.parts.forEach(function(part, i) {
                partFiles.push(part.getFilename());
            });

            // start the multipart upload ...
            multipartUpload(self.S3, destBucket, key, partFiles, cb);
        }]
    }, function(err, results) {
        uploadCB(err, true);
    });
};

CombinedObject.prototype.debugPrint = function() {
    var sumFrags = _.reduce(this.parts, function(m, v) { 
        return m + v.fragments.length; }, 0);

    dPartListObject("Combined Object #%d, size: %d bytes, %d parts, %d frags", 
            this.num, this.size, this.parts.length, sumFrags);

    this.parts.forEach(function(part, i) {
        dPartListPart("    - Part #%d, size: %d bytes, %d frags", 
            part.num, part.size, part.fragments.length);

        part.fragments.forEach(function(frag, i) {
            dPartListFrag("        - Frag %d, size: %d bytes (%s) ", 
                i, frag.size, frag.etag);
        });
    });
}


module.exports = CombinedObject;
