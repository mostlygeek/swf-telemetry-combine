#!/usr/bin/env python
import workers
import settings

w = workers.CreateCombineList(domain=settings.DOMAIN)

while w.run(): pass

