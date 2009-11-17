#!/usr/bin/python

import time, sys
from FTBEventAction import *

fea = FTBEventAction()
sHandle = fea.register('0.5',
                       'FTB.FTB_EXAMPLES.watchdog',
                       'trial-watchdog',
                       '0',
                       'FTB_SUBSCRIPTION_POLLING',
                       0)

# cEvent = fea.FTB_receive_event_t()
# eHandle = fea.FTB_event_handle_t()
# fea.raiseEvent('WATCH_DOG_EVENT', eHandle)
# time.sleep(2)
# fea.FTB_Poll_event(sHandle,cEvent)
# print cEvent.event_space

while(True):
    cEvent = fea.FTB_receive_event_t()
    time.sleep(3)
    if (fea.pollEvent(sHandle, cEvent)):
	print cEvent.event_name

