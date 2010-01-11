#!/usr/bin/python

import time, sys, random
#from FTBEventAction import *
from ftb import *

ftb = FTB()

ftb.FTB_Connect("0.5", "FTB.APP01.ES01", "app03", "0", "FTB_SUBSCRIPTION_NONE", 0)
ftb.FTB_Declare_publishable_events( None, [ ["ETYPE03", "INFO"] ], 1);

# shandle=ftb.FTB_subscribe_handle_t()
# ftb.FTB_Subscribe( shandle,"event_name=E03", None, None)

while True:
	random.seed(time.time())
	ehandle = ftb.FTB_event_handle_t()
	ftb.FTB_Publish("ETYPE03",ehandle);
	print 'Published: ETYPE03',  
	sleepDuration = random.randint(0,10)
	print ' Sleeping for %d msec' % (sleepDuration * 1000)
	time.sleep(sleepDuration)
# 	ehandle = ftb.FTB_event_handle_t()
# 	ftb.FTB_Publish("E02",ehandle);
# 	print 'Published: E02' 
# 	time.sleep(2)

