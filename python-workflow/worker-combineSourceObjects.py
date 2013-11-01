#!/usr/bin/env python

import workers
import settings

w = workers.CombineSourceObjects(domain=settings.DOMAIN)

while w.run(): pass

