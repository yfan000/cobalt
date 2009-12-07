#!/usr/bin/python

import time, sys
#from FTBEventAction import *
from ftb import *

ftb = FTB()

ftb.FTB_Connect("0.5", "FTB.FTB_EXAMPLES.watchdog", "trial-watchdog", "0", "FTB_SUBSCRIPTION_POLLING", 0)
ftb.FTB_Declare_publishable_events( None, [ ["WATCH_DOG_EVENT", "INFO"] ], 1);

shandle=ftb.FTB_subscribe_handle_t()
ftb.FTB_Subscribe( shandle,"event_space=ftb.all.watchdog", None, None)

while True:
	ehandle = ftb.FTB_event_handle_t()
	ftb.FTB_Publish("WATCH_DOG_EVENT",ehandle);
	print 'Published.'
	time.sleep(2)
