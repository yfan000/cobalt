#!/usr/bin/python

import time, sys
from FTBEventAction import *

fea = FTBEventAction()
ftb, sHandle = fea.register('0.5',
                       'FTB.FTB_EXAMPLES.watchdog',
                       'trial-watchdog',
                       '0',
                       'FTB_SUBSCRIPTION_POLLING',
                       0)

while(True):
    cEvent = fea.FTB_receive_event_t()
    time.sleep(3)
    fea.pollEvent(sHandle, cEvent)
    if cEvent.event_name == '':
	continue
    print cEvent.event_name

