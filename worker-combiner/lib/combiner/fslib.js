var fs = require('fs') 
    , async = require('async') 
    , crypto = require('crypto')
    , debug = require('debug')
    , d = debug("fs:combine");
    ;

module.exports = {

    "md5File" : function(filename, cb) {
        var s = fs.ReadStream(filename);
        var md5 = crypto.createHash('md5');

        s.on('data', function(d) {
            md5.update(d);
        });

        s.on('end', function() {
            cb(md5.digest('hex'));
        });
    }

    /**
     * so we have consistent naming of fragements
     */
    , "makeFragName" : function(baseDir, fragment) {
        var s = crypto.createHash('md5')
                      .update(fragment.key, 'utf8')
                      .digest('hex');

        return baseDir + '/' + fragment.etag + ".frag";
    }

    /*
     * Combines a bunch of fragments into a single file
     */
    , "combineFragments": function(outFile, sourceDir, frags, combineCB) {
        var self = this;


        async.series([
            // delete if something already there
            function(cb) {
                fs.exists(outFile, function(exists) {
                    if (exists === false) return cb(null);

                    d("Deleting %s", outFile);
                    fs.unlink(outFile, function(err) {
                        if (err) return cb(err);
                        cb(null)
                    });
                })
            } 

            // append all the fragments
            , function(cb) {
                function next(i) {
                    if (i === frags.length) return cb(null, true);

                    // get contents into RAM, write it out to the combined file
                    var sourceFile = self.makeFragName(sourceDir, frags[i]);
                    d("Combining %s into %s", sourceFile, outFile);
                    fs.readFile(sourceFile, function(err, data) {
                        if (err) return cb(err);

                        fs.appendFile(outFile, data, function(err) {
                            if (err) return cb(err);
                            next(i + 1);
                        });
                    });
                }
                next(0);
            }
        ],  combineCB);
    }
};
