#!/usr/bin/python

import time, sys
from FTBEventPolicy import *

class EventMonitor():
    policyDefinitions = {}

    def __init__(self):
	self.doRegister()

    def doRegister(self):
	fea = FTBEventPolicy()
	sHandle = fea.register('0.5',
                       'FTB.APP01.ES01',
                       'handler-01',
                       '0',
                       'FTB_SUBSCRIPTION_POLLING',
                       0)

    def startMonitor(self):
	self.policyDefinitions = fea.getPolicyDefinitions()

	while(True):
	    cEvent, ret = fea.pollEvent(sHandle)
	    if ret != FTB_SUCCESS:
		continue
	    eventAttrib = policyDefinitions[cEvent.event_name]
	    print 'Event-Type: ', eventAttrib.type, 
	    print ' Caught: ', cEvent.event_name


if __name__=='__main__':
	EventMonitor()
