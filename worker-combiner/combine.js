#!/usr/bin/env node

var program = require('commander')
    , AWS = require("aws-sdk")
    , async = require('async')
    , fetchFragmentList = require("./lib/combiner/fetchFragmentList")
    , CombinedObject = require('./lib/combiner/CombinedObject')
    ;
    
program
    .version('0.0.1')
    .option('-r, --region <region>', 'region of SWF, default: us-east-1', String, 'us-east-1')
    .option('-s, --swfdomain <domain>', 'SWF domain', String)
    .parse(process.argv);

if (!program.swfdomain) {
    console.log("domain required");
    process.exit();
}

AWS.config.update({
    accessKeyId : process.env.AWS_ACCESS_KEY,
    secretAccessKey : process.env.AWS_SECRET_KEY
});

//AWS.config.update({region: 'us-east-1'});
AWS.config.apiVersions = { s3 : '2006-03-01' };

var S3 = new AWS.S3({region:'us-east-1'})
    , swf = new AWS.SimpleWorkflow({apiVersion: '2012-01-25', region: program.region});


(function poll() {
    console.log("Polling for CombineObjects task");

    swf.pollForActivityTask({
        domain: program.swfdomain
        , taskList: {name: 'CombineObjects'}
        , identity: 'CombineWorker'
    }, function(err, activityTask) {
        if (err) {
            console.log("ERROR", err);
            return poll();
        }

        if (activityTask.startedEventId == 0) {
            console.log("Poll timed out");
            return poll();
        }

        // ohh actually something to do
        var input = JSON.parse(activityTask.input);
        console.log("Got Task, input: ", activityTask.input)
        combineObjects(
            '/tmp/work'
            , input.sourceBucket
            , input.outputBucket
            , input.outputKey
        , function(err, createdObjects) {
            if (err) { 
                // fail the job ... 
                console.log("Task Failed", err); 
                swf.respondActivityTaskFailed({
                    taskToken: activityTask.taskToken
                    , reason: "failed... "
                    , details: "a much longer description..."
                }, function(err) {
                    poll();
                });
            } else {
                console.log("Created Objects", createdObjects.list);

                swf.respondActivityTaskCompleted({
                    taskToken: activityTask.taskToken
                    , result : JSON.stringify(createdObjects)
                }, function(err) {
                    console.log("Task Completed");
                    poll();
                });
            }
        });
    });

})();

function combineObjects(workDir, sourceBucket, jobInputBucket, jobInputKey, cb) {

    console.log("Fetching list of objects to combine...");
    fetchFragmentList(S3, jobInputBucket, jobInputKey, function(err, jobInput) {
        if (err) return console.log(err);

        // create combined files locally
        var fragment
            , combinedObjs = [new CombinedObject(S3, sourceBucket, workDir, 0)]
            , cObj = combinedObjs[0];
        

        for (i=0; i<jobInput.objects.length; i++) {
            fragment = jobInput.objects[i];

            if (cObj.addFragment(fragment) === false) { // It's FULL!
                // make a new one
                cObj = new CombinedObject(S3, sourceBucket, workDir, combinedObjs.length);
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
        var createdObjects = {
            bucket: sourceBucket
            , list: []
        };

        console.log("Combining and uploading " + combinedObjs.length + " new combined parts.");
        var uploadQueue = async.queue(function(combinedObj, doneCB) {
            var name = "_tmpCombined/" + jobInput.prefix + ".combined-" + combinedObj.num;

            console.log("Uploading: ", name);
            combinedObj.upload(sourceBucket, name, function(err, result) {
                createdObjects.list.push(name);
                doneCB();
            });
        }, 1); // do 1 at a time

        uploadQueue.push(combinedObjs);
        uploadQueue.drain = function() {
            /* need some error handling here */
            cb(null, createdObjects);
        }
    });
};
