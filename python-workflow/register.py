# register.py
import boto.swf.layer2 as swf
import settings
from boto.swf.exceptions import SWFTypeAlreadyExistsError, SWFDomainAlreadyExistsError

DOMAIN = settings.DOMAIN

registerables = []
registerables.append(swf.Domain(name=DOMAIN))

# register workflow types, these all do the same thing 
# but allow for different deciders
registerables.append(swf.WorkflowType(domain=DOMAIN, name='CombineObjects', version='NoSourceModify', task_list='WFHandsOff'))
registerables.append(swf.WorkflowType(domain=DOMAIN, name='CombineObjects', version='CopySource', task_list='WFCopySource'))
registerables.append(swf.WorkflowType(domain=DOMAIN, name='CombineObjects', version='CopyAndDeleteSource', task_list='WFCopyAndDelete'))

# workflow tasks
registerables.append(swf.ActivityType(domain=DOMAIN, name="CreateCombineList", version="0.1", task_list='CreateList'))
registerables.append(swf.ActivityType(domain=DOMAIN, name="CombineSourceObjects", version="0.1", task_list='CombineObjects'))
registerables.append(swf.ActivityType(domain=DOMAIN, name="DeleteSourceObjects", version="0.1", task_list='DeleteSourceObjects'))
registerables.append(swf.ActivityType(domain=DOMAIN, name="CopySourceObjects", version="0.1", task_list='CopySourceObjects'))

for swf_entity in registerables:
    try:
        swf_entity.register()
        print swf_entity.name, 'registered successfully'
    except (SWFDomainAlreadyExistsError, SWFTypeAlreadyExistsError):
        print swf_entity.__class__.__name__, swf_entity.name, 'already exists'
