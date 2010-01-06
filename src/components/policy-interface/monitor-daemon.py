#!/usr/bin/python

import time, sys
from EventPolicy import *

fea = FTBEventPolicy()
sHandle = fea.register('0.5',
                       'EVENT-SPACE-01',
                       'monitor-daemon-02',
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



