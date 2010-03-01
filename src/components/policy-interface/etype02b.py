#!/usr/bin/python

import time, sys
#from FTBEventAction import *
from ftb import *

ftb = FTB()
event='MULTIPLE_FAIL'


ftb.FTB_Connect("0.5", "FTB.COBALT.DEMO", "app02b", "1", "FTB_SUBSCRIPTION_NONE", 0)
ftb.FTB_Declare_publishable_events( None, [ [event, "INFO"] ], 1);

# shandle=ftb.FTB_subscribe_handle_t()
# ftb.FTB_Subscribe( shandle,"event_name=E01", None, None)

while True:
	ehandle = ftb.FTB_event_handle_t()
	ftb.FTB_Publish(event,ehandle);
	print 'Published: %s' % (event) 
	time.sleep(3)
# 	ehandle = ftb.FTB_event_handle_t()
# 	ftb.FTB_Publish("E02",ehandle);
# 	print 'Published: E02' 
# 	time.sleep(2)

