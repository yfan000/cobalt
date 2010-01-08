#!/usr/bin/python

import time, sys
from FTBEventPolicy import *

class EventMonitor():
    ftbEventPolicy = None
    policyDefinitions = {}

    def __init__(self):
	sHandle = self.doRegister()
	self.startMonitor(sHandle)

    def doRegister(self):
	self.ftbEventPolicy = FTBEventPolicy()
	sHandle = self.ftbEventPolicy.register('0.5',
                       'FTB.APP01.ES01',
                       'handler-01',
                       '0',
                       'FTB_SUBSCRIPTION_POLLING',
                       0)
	return sHandle

    def startMonitor(self, sHandle):
	self.policyDefinitions = self.ftbEventPolicy.getPolicyDefinitions()

	while(True):
	    cEvent, ret = self.ftbEventPolicy.pollEvent(sHandle)
	    if ret != FTB_SUCCESS:
		continue
	    eventAttrib = self.policyDefinitions[cEvent.event_name]
	    print 'Event-Type: ', eventAttrib.type, 
	    print ' Caught: ', cEvent.event_name


if __name__=='__main__':
	EventMonitor()
