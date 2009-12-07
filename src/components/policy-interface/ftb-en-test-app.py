#!/usr/bin/python

import time
from FTBEventAction import *

fea = FTBEventAction()
f, sHandle = fea.register('0.5',
                       'FTB.FTB_EXAMPLES.watchdog',
                       'trial-watchdog',
                       '0',
                       'FTB_SUBSCRIPTION_POLLING',
                       0)



#fea.raiseEvent("WATCH_DOG_EVENT",eHandle)
while(True):
	eHandle = f.FTB_event_handle_t()
	f.FTB_Poll_event(sHandle, eHandle)
	print eHandle.event_name
	time.sleep(2)
