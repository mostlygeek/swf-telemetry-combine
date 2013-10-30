/*
 * Callback will be given: 
 * [
 *  {etag: "String", key: "String", size: Number}
 *  ...
 * ]
 */
module.exports = function(s3, bucket, key, cb) {
    s3.getObject({Bucket: bucket, Key: key}, function(err, data) {
        if (err) return cb(err);
        try {
            var list = JSON.parse(data.Body.toString());
            cb(null, list);
        } catch (err) {
            cb(err); // json parse error
        }
    })
}
