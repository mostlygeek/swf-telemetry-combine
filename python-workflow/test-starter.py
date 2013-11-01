import boto.swf.layer2 as swf
import settings


wkf = swf.WorkflowType(
        domain=settings.DOMAIN
        , name="CombineObjects"
        , version='NoSourceModify'
        , task_list='WFHandsOff'
        )

print "Starting new Workflow"
wkf.start(
    # what prefix to start combining for
    #input="idle_daily/Firefox/nightly/27.0a1"
    #input='idle_daily/Fennec/nightly/27.0a1/201309'
    input='idle_daily/Fennec/nightly/27.0a1/201310'
)
