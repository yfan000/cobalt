#!/usr/bin/python

import time
from FTBEventAction import *

fea = FTBEventAction()
ftb, sHandle = fea.register('0.5',
             'FTB.FTB_EXAMPLES.watchdog',
             'trial-watchdog',
             '0',
             'FTB_SUBSCRIPTION_POLLING',
             0)

cEvent = fea.FTB_receive_event_t()
eHandle = fea.FTB_event_handle_t()
ftb.FTB_Publish("WATCH_DOG_EVENT",eHandle)
time.sleep(2)
fea.FTB_Poll_event(sHandle,cEvent)

print cEvent.event_space
