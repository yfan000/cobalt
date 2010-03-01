#!/usr/bin/python

import time, sys, random
#from FTBEventAction import *
from ftb import *

ftb = FTB()
event='HYBRID_FAIL'

ftb.FTB_Connect("0.5", "FTB.COBALT.DEMO", "app03", "0", "FTB_SUBSCRIPTION_NONE", 0)
ftb.FTB_Declare_publishable_events( None, [ [event, "INFO"] ], 1);

# shandle=ftb.FTB_subscribe_handle_t()
# ftb.FTB_Subscribe( shandle,"event_name=E03", None, None)

while True:
	random.seed(time.time())
	ehandle = ftb.FTB_event_handle_t()
	ftb.FTB_Publish(event, ehandle);
	print 'Published: %s' % (event),   
	sleepDuration = random.randint(0,10)
	print ' Sleeping for %d msec' % (sleepDuration * 1000)
	time.sleep(sleepDuration)


