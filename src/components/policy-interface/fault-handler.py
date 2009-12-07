#!/usr/bin/python

import time, sys
from FTBEventAction import *

fea = FTBEventAction()
sHandle = fea.register('0.5',
                       'TEST.TEST00.es01',
                       'handler-01',
                       '0',
                       'FTB_SUBSCRIPTION_POLLING',
                       0)

while(True):
    cEvent, ret = fea.pollEvent(sHandle)
    if ret != FTB_SUCCESS:
	continue
    print 'Caught: ', cEvent.event_name,
    print ' Event Severity: ' + cEvent.severity,
    print ' Taking action: ' + fea.getEventAction(cEvent.event_name)



