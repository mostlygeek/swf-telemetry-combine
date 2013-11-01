#!/usr/bin/env python
import time
import boto.swf.layer2 as swf
import settings
import workers
import json

class CombineObjects(swf.Decider):

    domain = settings.DOMAIN
    task_list = 'WFHandsOff'
    version = 'NoSourceModify'

    def run(self):
        history = self.poll()

        # poll timed out (60s) w/ nothing to do
        if not 'events' in history: return True

        workflow_events = [e for e in history['events']
                           if not e['eventType'].startswith('Decision')]

        decisions = swf.Layer1Decisions()

        # Record latest non-decision event.
        last_event = workflow_events[-1]
        last_event_type = last_event['eventType']

        if last_event_type == 'WorkflowExecutionStarted':
            startInput = last_event['workflowExecutionStartedEventAttributes'].get('input')
            print "Scheduling First workflow action, prefix: %s" % startInput
            # Schedule the first activity.
            activityId = '%s-%s' % (workers.CreateCombineList.name, time.time())

            input = {
                'sourceBucket': 'telemetry-test-bucket'
                , 'prefix'    : startInput
                , 'outputBucket': 'telemetry-test-bucket'
            }

            decisions.schedule_activity_task(
                activityId
                , workers.CreateCombineList.name
                , workers.CreateCombineList.version
                , workers.CreateCombineList.task_list
                , input = json.dumps(input)
            )

        elif last_event_type == 'ActivityTaskFailed':
            print json.dumps(last_event)

            reason = last_event['activityTaskFailedEventAttributes']['reason']

            if reason == 'no-files':
                print "Nothing to do. Ending workflow."
                decisions.complete_workflow_execution(result='Nothing to do. Ending early.')
            else:
                # probably need something a bit more robust here w/ retry
                # logic ...
                decisions.fail_workflow_execution(reason=reason)

        elif last_event_type == 'ActivityTaskCompleted':
            # Take decision based on the name of activity that has just completed.
            # 1) Get activity's event id.
            last_event_attrs = last_event['activityTaskCompletedEventAttributes']
            completed_activity_id = last_event_attrs['scheduledEventId'] - 1

            # 2) Extract its name.
            activity_data = history['events'][completed_activity_id]
            activity_attrs = activity_data['activityTaskScheduledEventAttributes']
            activity_name = activity_attrs['activityType']['name']

            # 3) Optionally, get the result from the activity.
            result = last_event['activityTaskCompletedEventAttributes'].get('result')

            if activity_name == workers.CreateCombineList.name:
                print 'Scheduling activity: %s ver: %s' % (
                        workers.CombineSourceObjects.name, workers.CombineSourceObjects.version
                        )
                activityId = '%s-%s' % (workers.CombineSourceObjects.name, time.time())
                decisions.schedule_activity_task(
                        activityId
                        , workers.CombineSourceObjects.name
                        , workers.CombineSourceObjects.version
                        , workers.CombineSourceObjects.task_list
                        , input=result
                        )

            if activity_name == workers.CombineSourceObjects.name:
                print "Completing Workflow"
                decisions.complete_workflow_execution()

        self.complete(decisions=decisions)
        return True

if __name__ == "__main__":
    c = CombineObjects()

    print "polling for decisions"
    while c.run():
        print "polling again for decisions"


